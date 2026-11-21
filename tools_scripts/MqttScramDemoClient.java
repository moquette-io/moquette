///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS com.hivemq:hivemq-mqtt-client:1.4.2
//DEPS com.ongres.scram:scram-client:3.1
//DEPS info.picocli:picocli:4.7.7
//JAVA 11+

// NOTE: this is only a DEMO script, meant to prove that the Moquette broker works
// with MQTT5 enhanced authentication using SCRAM-SHA-256. It is not meant for production use.
//
// Usage: jbang MqttScramDemoClient.java [-h host] [-p port] [-u username] [--password password]
// The user must be registered in the broker's config/users.yaml (see ScramCredentialsGenerator).

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConfig;
import com.hivemq.client.mqtt.mqtt5.auth.Mqtt5EnhancedAuthMechanism;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5Auth;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5AuthBuilder;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5EnhancedAuthBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5Disconnect;
import com.ongres.scram.client.ScramClient;
import com.ongres.scram.common.exception.ScramException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

@Command(name = "MqttScramDemoClient", mixinStandardHelpOptions = true, helpCommand = false,
    description = "Demo MQTT5 client that connects to a broker using SCRAM-SHA-256 enhanced authentication.",
    addMethodSubcommands = false)
public class MqttScramDemoClient implements Callable<Integer> {

    @Option(names = {"-H", "--host"}, defaultValue = "localhost", description = "Broker host (default: ${DEFAULT-VALUE})")
    private String host;

    @Option(names = {"-p", "--port"}, defaultValue = "1883", description = "Broker port (default: ${DEFAULT-VALUE})")
    private int port;

    @Option(names = {"-u", "--username"}, defaultValue = "alice", description = "Username (default: ${DEFAULT-VALUE})")
    private String username;

    @Option(names = {"-P", "--password"}, defaultValue = "secret", description = "Password (default: ${DEFAULT-VALUE})")
    private String password;

    public static void main(String[] args) {
        System.exit(new CommandLine(new MqttScramDemoClient()).execute(args));
    }

    @Override
    public Integer call() {
        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier("scram_demo_client")
            .serverHost(host)
            .serverPort(port)
            .enhancedAuth(new ScramAuthMechanism(username, password))
            .buildBlocking();

        try {
            final Mqtt5ConnAck connAck = client.connect();
            System.out.println("Connected to " + host + ":" + port + " as '" + username + "', reason code: "
                + connAck.getReasonCode());
            client.disconnect();
            return 0;
        } catch (Exception e) {
            System.err.println("Connection failed: " + e);
            return 1;
        }
    }

    /**
     * Adapts the SCRAM client of com.ongres.scram:scram-client to the HiveMQ client enhanced authentication.
     * An instance handles a single authentication.
     */
    static class ScramAuthMechanism implements Mqtt5EnhancedAuthMechanism {

        private static final String MECHANISM_NAME = "SCRAM-SHA-256";

        private enum State { INITIAL, CLIENT_FIRST_SENT, CLIENT_FINAL_SENT, SERVER_VERIFIED }

        private final ScramClient scramClient;
        private State state = State.INITIAL;

        ScramAuthMechanism(String username, String password) {
            this.scramClient = ScramClient.builder()
                .advertisedMechanisms(Collections.singletonList(MECHANISM_NAME))
                .username(username)
                .password(password.toCharArray())
                .build();
        }

        @Override
        public MqttUtf8String getMethod() {
            return MqttUtf8String.of(MECHANISM_NAME);
        }

        @Override
        public int getTimeout() {
            return 10; // seconds
        }

        @Override
        public CompletableFuture<Void> onAuth(Mqtt5ClientConfig config, Mqtt5Connect connect, Mqtt5EnhancedAuthBuilder authBuilder) {
            authBuilder.data(bytes(scramClient.clientFirstMessage().toString()));
            state = State.CLIENT_FIRST_SENT;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Boolean> onContinue(Mqtt5ClientConfig config, Mqtt5Auth auth, Mqtt5AuthBuilder authBuilder) {
            try {
                switch (state) {
                    case CLIENT_FIRST_SENT:
                        scramClient.serverFirstMessage(text(auth));
                        authBuilder.data(bytes(scramClient.clientFinalMessage().toString()));
                        state = State.CLIENT_FINAL_SENT;
                        return CompletableFuture.completedFuture(true);
                    case CLIENT_FINAL_SENT:
                        // throws if the server reports an error or the server signature is not valid
                        scramClient.serverFinalMessage(text(auth));
                        authBuilder.data(new byte[0]);
                        state = State.SERVER_VERIFIED;
                        return CompletableFuture.completedFuture(true);
                    default:
                        return failed(new IllegalStateException("Unexpected AUTH in state " + state));
                }
            } catch (ScramException e) {
                return failed(e);
            }
        }

        @Override
        public CompletableFuture<Boolean> onAuthSuccess(Mqtt5ClientConfig config, Mqtt5ConnAck connAck) {
            // accept the connection only if the server has been verified
            return CompletableFuture.completedFuture(state == State.SERVER_VERIFIED);
        }

        @Override
        public CompletableFuture<Void> onReAuth(Mqtt5ClientConfig config, Mqtt5AuthBuilder authBuilder) {
            return failed(new IllegalStateException("Re-authentication not supported"));
        }

        @Override
        public CompletableFuture<Boolean> onServerReAuth(Mqtt5ClientConfig config, Mqtt5Auth auth, Mqtt5AuthBuilder authBuilder) {
            return failed(new IllegalStateException("Re-authentication not supported"));
        }

        @Override
        public CompletableFuture<Boolean> onReAuthSuccess(Mqtt5ClientConfig config, Mqtt5Auth auth) {
            return failed(new IllegalStateException("Re-authentication not supported"));
        }

        @Override
        public void onAuthRejected(Mqtt5ClientConfig config, Mqtt5ConnAck connAck) {
        }

        @Override
        public void onReAuthRejected(Mqtt5ClientConfig config, Mqtt5Disconnect disconnect) {
        }

        @Override
        public void onAuthError(Mqtt5ClientConfig config, Throwable cause) {
        }

        @Override
        public void onReAuthError(Mqtt5ClientConfig config, Throwable cause) {
        }

        private static <T> CompletableFuture<T> failed(Throwable cause) {
            final CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(cause);
            return future;
        }

        private static byte[] bytes(String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }

        private static String text(Mqtt5Auth auth) {
            final ByteBuffer data = auth.getData().orElseThrow(() -> new IllegalStateException("AUTH without data"));
            final byte[] content = new byte[data.remaining()];
            data.duplicate().get(content);
            return new String(content, StandardCharsets.UTF_8);
        }
    }
}

package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.integration.IntegrationUtils;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.moquette.integration.mqtt5.ConnectTest.assertConnectionAccepted;
import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractServerIntegrationTest {
    Server broker;
    IConfig config;

    @TempDir
    Path tempFolder;
    protected String dbPath;

    Client lowLevelClient;

    @NotNull
    static Mqtt5BlockingClient createSubscriberClient(String clientId) {
        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, client.connect().getReasonCode(), clientId + " connected");
        return client;
    }

    @NotNull
    static Mqtt5BlockingClient createPublisherClient() {
        return AbstractSubscriptionIntegrationTest.createClientWithStartFlagAndClientId(true, "publisher");
    }

    protected static void verifyNoPublish(Mqtt5BlockingClient subscriber, Consumer<Void> action, Duration timeout, String message) throws InterruptedException {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            action.accept(null);
            Optional<Mqtt5Publish> publishedMessage = publishes.receive(timeout.getSeconds(), TimeUnit.SECONDS);

            // verify no published will in 10 seconds
            assertFalse(publishedMessage.isPresent(), message);
        }
    }

    protected static void verifyPublishedMessage(Mqtt5BlockingClient client, Consumer<Void> action, MqttQos expectedQos,
                                                 String expectedPayload, String errorMessage, int timeoutSeconds) throws Exception {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            action.accept(null);
            Optional<Mqtt5Publish> publishMessage = publishes.receive(timeoutSeconds, TimeUnit.SECONDS);
            if (!publishMessage.isPresent()) {
                fail("Expected to receive a publish message");
                return;
            }
            Mqtt5Publish msgPub = publishMessage.get();
            final String payload = new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8);
            assertEquals(expectedPayload, payload, errorMessage);
            assertEquals(expectedQos, msgPub.getQos());
        }
    }

    static void verifyOfType(MqttMessage received, MqttMessageType mqttMessageType) {
        assertEquals(mqttMessageType, received.fixedHeader().messageType());
    }

    static void verifyPublishMessage(Mqtt5BlockingClient subscriber, Consumer<Mqtt5Publish> assertion) throws InterruptedException {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            Optional<Mqtt5Publish> publishMessage = publishes.receive(1, TimeUnit.SECONDS);
            if (!publishMessage.isPresent()) {
                fail("Expected to receive a publish message");
                return;
            }
            Mqtt5Publish msgPub = publishMessage.get();
            assertion.accept(msgPub);
        }
    }

    @NotNull
    Mqtt5BlockingClient createSubscriberClient() {
        String clientId = clientName();
        return createSubscriberClient(clientId);
    }

    public abstract String clientName();

    protected void startServer(String dbPath) throws IOException {
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        broker.startServer(config);
    }

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.ONE_SECOND);
    }

    @BeforeEach
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);

        lowLevelClient = new Client("localhost").clientId(clientName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        lowLevelClient.shutdownConnection();
        stopServer();
    }

    protected void stopServer() {
        broker.stopServer();
    }

    void restartServerWithSuspension(Duration timeout) throws InterruptedException, IOException {
        stopServer();
        Thread.sleep(timeout.toMillis());
        startServer(dbPath);
    }

    void connectLowLevel() {
        MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");
    }

    void connectLowLevel(int keepAliveSecs) {
        MqttConnAckMessage connAck = lowLevelClient.connectV5(keepAliveSecs);
        assertConnectionAccepted(connAck, "Connection must be accepted");
    }
}

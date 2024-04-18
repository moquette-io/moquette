package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5Disconnect;
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5DisconnectReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5WillPublishBuilder;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.*;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.*;

class ConnectTest extends AbstractServerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectTest.class);

    private ScheduledExecutorService scheduleTasks;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        scheduleTasks = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    public void tearDown() throws Exception {
        scheduleTasks.shutdown();
        super.tearDown();
    }

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void simpleConnect() {
        Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier("simple_connect_test")
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        final Mqtt5ConnAck connectAck = client.connect();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, connectAck.getReasonCode(), "Accept plain connection");

        client.disconnect();
    }

    @Test
    public void sendConnectOnDisconnectedConnection() {
        MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");
        lowLevelClient.disconnect();

        try {
            lowLevelClient.connectV5();
            fail("Connect on Disconnected TCP socket can't happen");
        } catch (RuntimeException rex) {
            assertEquals("Cannot receive ConnAck in 2 s", rex.getMessage());
        }
    }

    @Test
    public void receiveInflightPublishesAfterAReconnect() {
        final Mqtt5BlockingClient publisher = MqttClient.builder()
            .useMqttVersion5()
            .identifier("publisher")
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        Mqtt5ConnAck connectAck = publisher.connect();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, connectAck.getReasonCode(), "Publisher connected");

        final MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");
        lowLevelClient.subscribe("/test", MqttQoS.AT_LEAST_ONCE);

        final Mqtt5PublishResult pubResult = publisher.publishWith()
            .topic("/test")
            .qos(com.hivemq.client.mqtt.datatypes.MqttQos.AT_LEAST_ONCE)
            .payload("Hello".getBytes(StandardCharsets.UTF_8))
            .send();
        assertFalse(pubResult.getError().isPresent(), "Publisher published");

        lowLevelClient.disconnect();

        // reconnect the raw subscriber
        final Client reconnectingSubscriber = new Client("localhost").clientId("subscriber");
        assertConnectionAccepted(reconnectingSubscriber.connectV5(), "Connection must be accepted");

        Awaitility.await()
            .atMost(2, TimeUnit.SECONDS)
            .until(reconnectingSubscriber::hasReceivedMessages);

        Optional<MqttPublishMessage> opt = reconnectingSubscriber.nextQueuedMessage()
            .filter(m -> m instanceof MqttPublishMessage)
            .map(m -> (MqttPublishMessage) m);
        final String publishPayload;
        if (opt.isPresent()) {
            publishPayload = opt.get().payload().toString(StandardCharsets.UTF_8);
            opt.get().release();
        } else {
            publishPayload = "Fake Payload";
        }
        assertEquals("Hello", publishPayload, "The inflight payload from previous subscription MUST be received");

        reconnectingSubscriber.disconnect();
    }

    public static void assertConnectionAccepted(MqttConnAckMessage connAck, String message) {
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, connAck.variableHeader().connectReturnCode(), message);
    }

    @Test
    public void fireWillAfterTheDelaySpecifiedInConnectProperties() throws InterruptedException {
        final Mqtt5BlockingClient clientWithWill = createAndConnectClientWithWillTestament("simple_client");

        final Mqtt5BlockingClient testamentSubscriber = createAndConnectClientListeningToTestament();

        // schedule a bad disconnect
        scheduleDisconnectWithErrorCode(clientWithWill, Duration.ofMillis(500));

        verifyPublishedMessage(testamentSubscriber, 10, "Will message must be received");
    }

    @Test
    public void fireWillAfterTheDelaySpecifiedInConnectPropertiesAndMessageExpiry() throws InterruptedException {
        int messageExpiry = 5;
        final Mqtt5BlockingClient clientWithWill =
            createAndConnectClientWithWillTestamentAndMessageExpiry("simple_client", 1, messageExpiry);

        final Mqtt5BlockingClient testamentSubscriber = createAndConnectClientListeningToTestament();

        // schedule a bad disconnect
        scheduleDisconnectWithErrorCode(clientWithWill, Duration.ofMillis(500));

        TestUtils.verifyPublishedMessage(testamentSubscriber, 10,
            (Mqtt5Publish message) -> {
                final String payload = new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8);
                assertEquals("Goodbye", payload, "Will message must be received");

                long expiry = message.getMessageExpiryInterval().orElse(-1L);
                assertEquals(messageExpiry, expiry);
            });
    }

    private static void verifyPublishedMessage(Mqtt5BlockingClient subscriber, int timeout, String message) throws InterruptedException {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            Optional<Mqtt5Publish> publishMessage = publishes.receive(timeout, TimeUnit.SECONDS);
            final String payload = publishMessage.map(Mqtt5Publish::getPayloadAsBytes)
                .map(b -> new String(b, StandardCharsets.UTF_8))
                .orElse("Failed to load payload");
            assertEquals("Goodbye", payload, message);
        }
    }

    @Test
    public void avoidToFirePreviouslyScheduledWillWhenSameClientIDReconnects() throws InterruptedException {
        final String clientId = "simple_client";
        final Mqtt5BlockingClient clientWithWill = createAndConnectClientWithWillTestament(clientId);

        final Mqtt5BlockingClient testamentSubscriber = createAndConnectClientListeningToTestament();

        // client trigger a will message, disconnecting with bad reason code
        final Mqtt5Disconnect malformedPacketReason = Mqtt5Disconnect.builder()
            .reasonCode(Mqtt5DisconnectReasonCode.MALFORMED_PACKET)
            .build();
        clientWithWill.disconnect(malformedPacketReason);

        // wait no will is published
        verifyNoTestamentIsPublished(testamentSubscriber, unused -> {
            // reconnect another client with same clientId
            final Mqtt5BlockingClient client = MqttClient.builder()
                .useMqttVersion5()
                .identifier(clientId)
                .serverHost("localhost")
                .serverPort(1883)
                .buildBlocking();
            Mqtt5ConnAck connectAck = client.connect();
            assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, connectAck.getReasonCode(), "Client connected");

        }, Duration.ofSeconds(10));
    }

    private static void verifyNoTestamentIsPublished(Mqtt5BlockingClient testamentSubscriber, Consumer<Void> action, Duration timeout) throws InterruptedException {
        verifyNoPublish(testamentSubscriber, action, timeout, "No will message should be published");
    }

    @Test
    public void fireWillWhenSessionExpiresIfItHappenBeforeWillDelay() throws InterruptedException {
        final String clientId = "simple_client";
        // create a client with will delay greater than session expiry
        final Mqtt5BlockingClient clientWithWill = createAndConnectClientWithWillTestament(clientId,  10, 60);

        final Mqtt5BlockingClient testamentSubscriber = createAndConnectClientListeningToTestament();

        // close the session with some error return code
        final Mqtt5Disconnect malformedPacketReason = Mqtt5Disconnect.builder()
            .reasonCode(Mqtt5DisconnectReasonCode.MALFORMED_PACKET)
            .build();
        clientWithWill.disconnect(malformedPacketReason);

        verifyPublishedMessage(testamentSubscriber, 20, "Will message must be received");
    }

    @Test
    public void noWillMessageIsFiredOnNormalDisconnection() throws InterruptedException {
        final String clientId = "simple_client";
        // create a client with will delay greater than session expiry
        final Mqtt5BlockingClient clientWithWill = createAndConnectClientWithWillTestament(clientId,  10, 60);

        final Mqtt5BlockingClient testamentSubscriber = createAndConnectClientListeningToTestament();

        // wait no will is published
        verifyNoTestamentIsPublished(testamentSubscriber, unused -> {
            // normal session disconnection
            clientWithWill.disconnect(Mqtt5Disconnect.builder().build());
        }, Duration.ofSeconds(10));
    }

    @Test
    public void givenClientWithWillThatCleanlyDisconnectsWithWillShouldTriggerTheTestamentMessage() throws InterruptedException {
        final String clientId = "simple_client";
        // create a client with will delay greater than session expiry
        final Mqtt5BlockingClient clientWithWill = createAndConnectClientWithWillTestament(clientId,  10, 60);

        final Mqtt5BlockingClient testamentSubscriber = createAndConnectClientListeningToTestament();

        // wait no will is published
        verifyNoTestamentIsPublished(testamentSubscriber, unused -> {
            // normal session disconnection with will
            clientWithWill.disconnect(Mqtt5Disconnect.builder()
                .reasonCode(Mqtt5DisconnectReasonCode.DISCONNECT_WITH_WILL_MESSAGE)
                .build());
        }, Duration.ofSeconds(10));
    }

    @Test
    public void delayedWillIsSentAlsoAfterAServerRestart() throws InterruptedException, IOException {
        final String clientId = "simple_client";
        final Mqtt5BlockingClient clientWithWill = createAndConnectClientWithWillTestament(clientId, 10);

        // client trigger a will message, disconnecting with bad reason code
        final Mqtt5Disconnect malformedPacketReason = Mqtt5Disconnect.builder()
            .reasonCode(Mqtt5DisconnectReasonCode.MALFORMED_PACKET)
            .build();
        clientWithWill.disconnect(malformedPacketReason);

        restartServerWithSuspension(Duration.ofSeconds(5));

        final Mqtt5BlockingClient testamentSubscriber = createAndConnectClientListeningToTestament();

        verifyPublishedMessage(testamentSubscriber, 10, "Will message must be received after server restart");
    }

    private void scheduleDisconnectWithErrorCode(Mqtt5BlockingClient clientWithWill, Duration delay) {
        scheduleTasks.schedule(() -> {
            // disconnect in a way that the will is triggered
            final Mqtt5Disconnect malformedPacketReason = Mqtt5Disconnect.builder()
                .reasonCode(Mqtt5DisconnectReasonCode.MALFORMED_PACKET)
                .build();
            clientWithWill.disconnect(malformedPacketReason);
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @NotNull
    private Mqtt5BlockingClient createAndConnectClientListeningToTestament() {
        final Mqtt5BlockingClient testamentSpy = MqttClient.builder()
            .useMqttVersion5()
            .identifier("testament_listener")
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, testamentSpy.connect().getReasonCode(), "Testament spy connected");
        testamentSpy.subscribeWith()
            .topicFilter("/will_testament")
            .qos(MqttQos.AT_MOST_ONCE)
            .send();
        return testamentSpy;
    }

    @NotNull
    private Mqtt5BlockingClient createAndConnectClientWithWillTestament(String clientId) {
        int delayInSeconds = 1;
        return createAndConnectClientWithWillTestament(clientId, delayInSeconds);
    }

    @NotNull
    private static Mqtt5BlockingClient createAndConnectClientWithWillTestament(String clientId, int delayInSeconds) {
        Mqtt5WillPublishBuilder.Nested.Complete<? extends Mqtt5ConnectBuilder> willPublishBuilder = deafaultWillBuilder(delayInSeconds);

        Mqtt5ConnectBuilder connectBuilder = willPublishBuilder.applyWillPublish();

        return createAndConnectWithBuilder(clientId, connectBuilder);
    }

    @NotNull
    private static Mqtt5WillPublishBuilder.Nested.Complete<? extends Mqtt5ConnectBuilder> deafaultWillBuilder(int delayInSeconds) {
        Mqtt5WillPublishBuilder.Nested.Complete<? extends Mqtt5ConnectBuilder> willPublishBuilder = Mqtt5Connect.builder()
            .keepAlive(10)
            .willPublish()
                .topic("/will_testament")
                .payload("Goodbye".getBytes(StandardCharsets.UTF_8))
                .delayInterval(delayInSeconds) // 1 second
                .contentType("something content type here")
                .userProperties()
                    .add("test_property", "value of a property")
                .applyUserProperties();
        return willPublishBuilder;
    }

    @NotNull
    private static Mqtt5BlockingClient createAndConnectClientWithWillTestamentAndMessageExpiry(String clientId,
                                                                                               int delayInSeconds,
                                                                                               int messageExpirySeconds) {
        Mqtt5WillPublishBuilder.Nested.Complete<? extends Mqtt5ConnectBuilder> willPublishBuilder = deafaultWillBuilder(delayInSeconds);

        willPublishBuilder.messageExpiryInterval(messageExpirySeconds);

        Mqtt5ConnectBuilder connectBuilder = willPublishBuilder.applyWillPublish();

        return createAndConnectWithBuilder(clientId, connectBuilder);
    }

    @NotNull
    private static Mqtt5BlockingClient createAndConnectClientWithWillTestament(String clientId, int sessionExpiryInSeconds,
                                                                               int delayInSeconds) {
        Mqtt5ConnectBuilder connectBuilder = Mqtt5Connect.builder()
            .keepAlive(10)
            .sessionExpiryInterval(sessionExpiryInSeconds)
            .willPublish()
            .topic("/will_testament")
            .payload("Goodbye".getBytes(StandardCharsets.UTF_8))
            .delayInterval(delayInSeconds) // 1 second
            .applyWillPublish();

        return createAndConnectWithBuilder(clientId, connectBuilder);
    }

    @NotNull
    private static Mqtt5BlockingClient createAndConnectWithBuilder(String clientId, Mqtt5ConnectBuilder connectBuilder) {
        final Mqtt5BlockingClient clientWithWill = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        Mqtt5ConnAck connectAck = clientWithWill.connect(connectBuilder.build());
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, connectAck.getReasonCode(), "Client must result connected");
        return clientWithWill;
    }

}

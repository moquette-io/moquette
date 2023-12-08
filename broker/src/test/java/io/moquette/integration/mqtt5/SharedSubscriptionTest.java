package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.DeclarativeAuthorizatorPolicy;
import io.moquette.broker.security.IAuthorizatorPolicy;
import io.moquette.integration.IntegrationUtils;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.moquette.integration.mqtt5.ConnectTest.assertConnectionAccepted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SharedSubscriptionTest extends AbstractServerIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SharedSubscriptionTest.class);

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenAClientSendingBadlyFormattedSharedSubscriptionNameThenItIsDisconnected() {
        connectLowLevel();

        MqttMessage received = lowLevelClient.subscribeWithError("$share/+/measures/temp", MqttQoS.AT_LEAST_ONCE);

        // verify received is a disconnect with an error
        verifyOfType(received, MqttMessageType.DISCONNECT);
        MqttReasonCodeAndPropertiesVariableHeader disconnectHeader = (MqttReasonCodeAndPropertiesVariableHeader) received.variableHeader();
        assertEquals(MqttReasonCodes.Disconnect.MALFORMED_PACKET.byteValue(), disconnectHeader.reasonCode());
    }

    private static void verifyOfType(MqttMessage received, MqttMessageType mqttMessageType) {
        assertEquals(mqttMessageType, received.fixedHeader().messageType());
    }

    private void connectLowLevel() {
        MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");
    }

    @Test
    public void whenAClientSendASharedSubscriptionThenReceivedSubscriptionACKWithTheSharedTopicFilter() {
        connectLowLevel();

        MqttMessage received = lowLevelClient.subscribeWithError("$share/metrics/measures/temp", MqttQoS.AT_LEAST_ONCE);

        verifyOfType(received, MqttMessageType.SUBACK);
        MqttSubAckMessage subAckMessage = (MqttSubAckMessage) received;
        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size(), "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.AT_LEAST_ONCE.value(), grantedQoSes.iterator().next());
    }

    @Test
    public void givenATopicNotReadableWhenAClientSubscribeSharedThenReceiveSubackWithNegativeResponse() throws IOException {
        // stop already started broker instance
        stopServer();

        LOG.info("Stopped existing server");
        final IAuthorizatorPolicy policy = new DeclarativeAuthorizatorPolicy.Builder().build();
        startServer(dbPath, policy);

        LOG.info("Started new server");

        // Connect the client to newly started broker
        lowLevelClient = new Client("localhost").clientId(clientName());

        connectLowLevel();

        MqttMessage received = lowLevelClient.subscribeWithError("$share/metrics/measures/temp", MqttQoS.AT_LEAST_ONCE);

        verifyOfType(received, MqttMessageType.SUBACK);
        MqttSubAckMessage subAckMessage = (MqttSubAckMessage) received;
        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size(),
            "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.FAILURE.value(), grantedQoSes.iterator().next(),
            "Not readable topic should reflect also in shared subscription");
    }

    protected void startServer(String dbPath, IAuthorizatorPolicy authPolicy) throws IOException {
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        broker.startServer(config, null, null, null, authPolicy);
    }

    @Test
    public void givenASharedSubscriptionClientReceivesANotification() throws InterruptedException {
        final Mqtt5BlockingClient subscriberClient = createSubscriberClient();
        subscriberClient.subscribeWith()
            .topicFilter("$share/collectors/metric/temperature/#")
            .send();

        Mqtt5BlockingClient publisherClient = createPublisherClient();
        publisherClient.publishWith()
                .topic("metric/temperature/living")
                .payload("18".getBytes(StandardCharsets.UTF_8))
                .send();

        verifyPublishedMessage(subscriberClient, 10, "18", "Shared message must be received");
    }

    @NotNull
    private Mqtt5BlockingClient createSubscriberClient() {
        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientName())
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, client.connect().getReasonCode(), "Subscriber connected");
        return client;
    }

    @NotNull
    private Mqtt5BlockingClient createPublisherClient() {
        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier("publisher")
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, client.connect().getReasonCode(), "Publisher connected");
        return client;
    }

    private static void verifyPublishedMessage(Mqtt5BlockingClient subscriber, int timeout, String expectedPayload, String message) throws InterruptedException {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            Optional<Mqtt5Publish> publishMessage = publishes.receive(timeout, TimeUnit.SECONDS);
            final String payload = publishMessage.map(Mqtt5Publish::getPayloadAsBytes)
                .map(b -> new String(b, StandardCharsets.UTF_8))
                .orElse("Failed to load payload");
            assertEquals(expectedPayload, payload, message);
        }
    }

    @Test
    public void whenAClientSubscribeToASharedTopicThenDoesntReceiveAnyRetainedMessagedOnTheMatchingTopicFilter() {
        fail("Implement");
    }
}

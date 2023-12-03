package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.netty.handler.codec.mqtt.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.moquette.integration.mqtt5.ConnectTest.assertConnectionAccepted;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SharedSubscriptionTest extends AbstractServerIntegrationTest {

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenAClientSendingBadlyFormattedSharedSubscriptionNameThenItIsDisconnected() {
        MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");

        MqttMessage received = lowLevelClient.subscribeWithError("$share/+/measures/temp", MqttQoS.AT_LEAST_ONCE);

        // verify received is a disconnect with an error
        assertEquals(MqttMessageType.DISCONNECT, received.fixedHeader().messageType());
        MqttReasonCodeAndPropertiesVariableHeader disconnectHeader = (MqttReasonCodeAndPropertiesVariableHeader) received.variableHeader();
        assertEquals(MqttReasonCodes.Disconnect.MALFORMED_PACKET.byteValue(), disconnectHeader.reasonCode());
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
}

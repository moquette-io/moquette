package io.moquette.integration;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.moquette.integration.mqtt5.AbstractServerIntegrationTest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class PublishTest extends AbstractServerIntegrationTest {
    @Override
    public String clientName() {
        return "Publisher";
    }

    @Test
    public void givenAConnectedClientWhenItPublishToReservedTopicNoPublishIsForwarded() throws InterruptedException {
        // given a connected client
        Mqtt3BlockingClient publisher = createHiveMqV3Client("publisher");

        Mqtt3BlockingClient subscriber = createHiveMqV3Client("subscriber");

        subscriber.subscribeWith()
            .topicFilter("$sys/monitor")
            .qos(MqttQos.AT_MOST_ONCE)
            .send();

        // when send a publish to reserved topic
        publisher.publishWith()
            .topic("$sys/monitor")
            .payload("Something new".getBytes(StandardCharsets.UTF_8))
            .send();

        // then no message is published
        verifyNoMessageIsReceived(subscriber, Duration.ofSeconds(2));
    }

    @NotNull
    private static Mqtt3BlockingClient createHiveMqV3Client(String clientId) {
        Mqtt3BlockingClient publisher = MqttClient.builder()
            .useMqttVersion3()
            .identifier(clientId)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        Mqtt3ConnAck connectAck = publisher.connect();
        assertEquals(Mqtt3ConnAckReturnCode.SUCCESS, connectAck.getReturnCode(), "Accept plain connection");
        return publisher;
    }

    private static void verifyMessageIsReceived(String expectedPayload, Mqtt3BlockingClient client, Duration timeout) throws InterruptedException {
        try (Mqtt3BlockingClient.Mqtt3Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Optional<Mqtt3Publish> publishMessage = publishes.receive(timeout.getSeconds(), TimeUnit.SECONDS);
            final String payload = publishMessage.map(Mqtt3Publish::getPayloadAsBytes)
                .map(b -> new String(b, StandardCharsets.UTF_8))
                .orElse("Failed to load payload");
            assertEquals(expectedPayload, payload, "Something new has to be received");
        }
    }

    private static void verifyNoMessageIsReceived(Mqtt3BlockingClient testamentSubscriber, Duration timeout) throws InterruptedException {
        try (Mqtt3BlockingClient.Mqtt3Publishes publishes = testamentSubscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            Optional<Mqtt3Publish> publishedWill = publishes.receive(timeout.getSeconds(), TimeUnit.SECONDS);

            // verify no published will in 10 seconds
            assertFalse(publishedWill.isPresent(), "No message should be published");
        }
    }


}

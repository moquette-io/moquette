package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class SubscriptionWithIdentifierTest extends AbstractSubscriptionIntegrationTest {

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenNonSharedSubscriptionWithIdentifierWhenPublishMatchedThenReceivesTheOriginalIdentifier() throws InterruptedException {
        connectLowLevel();

        // subscribe with an identifier
        MqttMessage received = lowLevelClient.subscribeWithIdentifier("/metrics/measures/temp",
            MqttQoS.AT_LEAST_ONCE, 123, 400, TimeUnit.MILLISECONDS);
        verifyOfType(received, MqttMessageType.SUBACK);

        Mqtt5BlockingClient publisher = createPublisherClient();
        publisher.publishWith()
            .topic("/metrics/measures/temp")
            .payload("17C°".getBytes(StandardCharsets.UTF_8))
            .send();

        // receive a publish message on the subscribed topic
        Awaitility.await()
            .atMost(2, TimeUnit.SECONDS)
            .until(lowLevelClient::hasReceivedMessages);
        MqttMessage mqttMsg = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        verifyOfType(mqttMsg, MqttMessageType.PUBLISH);
        MqttPublishMessage mqttPublish = (MqttPublishMessage) mqttMsg;
        verifySubscriptionIdentifier(123, mqttPublish);
        assertTrue(mqttPublish.release(), "Reference of publish should be released");
    }

    private static void verifySubscriptionIdentifier(int expectedSubscriptionIdentifier, MqttPublishMessage publish) {
        MqttProperties.MqttProperty<Integer> subscriptionIdentifierProperty = publish.variableHeader()
            .properties()
            .getProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value());
        assertNotNull(subscriptionIdentifierProperty, "subscription identifier property must be present");
        assertEquals(expectedSubscriptionIdentifier, subscriptionIdentifierProperty.value());
    }

    @Test
    public void givenNonSharedSubscriptionWithIdentifierWhenRetainedMessageMatchedThenReceivesTheOriginalIdentifier() throws InterruptedException {
        // send a retained publish
        Mqtt5BlockingClient publisher = createPublisherClient();
        publisher.publishWith()
            .topic("/metrics/measures/temp")
            .payload("17C°".getBytes(StandardCharsets.UTF_8))
            .retain(true)
            .qos(MqttQos.AT_LEAST_ONCE) // WARN: only QoS1 and QoS2 are effectively retained
            .send();
        publisher.disconnect();
        Thread.sleep(500); // give time to close the connection and save the message as retained

        connectLowLevel();

        // subscribe with an identifier
        MqttMessage received = lowLevelClient.subscribeWithIdentifier("/metrics/measures/temp",
            MqttQoS.AT_LEAST_ONCE, 123);
        verifyOfType(received, MqttMessageType.SUBACK);

        // receive a publish message on the subscribed topic
        Awaitility.await()
            .atMost(2, TimeUnit.SECONDS)
            .until(lowLevelClient::hasReceivedMessages);
        MqttMessage mqttMsg = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        verifyOfType(mqttMsg, MqttMessageType.PUBLISH);
        MqttPublishMessage mqttPublish = (MqttPublishMessage) mqttMsg;
        verifySubscriptionIdentifier(123, mqttPublish);
        assertTrue(mqttPublish.release(), "Reference of publish should be released");
    }
}

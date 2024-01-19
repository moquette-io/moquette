package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SubscriptionWithIdentifierTest extends AbstractSubscriptionIntegrationTest {

    @Override
    public String clientName() {
        return "subscriber";
    }

//    This fails while shouldn't because the MQTT properties are spread around (also on peristent layer)
    @Test
    public void givenNonSharedSubscriptionWithIdentifierWhenPublishMatchedThenReceivesTheOriginalIdentifier() throws InterruptedException {
        connectLowLevel();

        // subscribe with an identifier
        MqttMessage received = lowLevelClient.subscribeWithIdentifier("/metrics/measures/temp",
            MqttQoS.AT_LEAST_ONCE, 123);
        verifyOfType(received, MqttMessageType.SUBACK);

        Mqtt5BlockingClient publisher = createPublisherClient();
        publisher.publishWith()
            .topic("/metrics/measures/temp")
            .send();

        // receive a publish message on the subscribed topic
        Awaitility.await()
            .atMost(2, TimeUnit.SECONDS)
            .until(lowLevelClient::hasReceivedMessages);
        MqttMessage mqttMsg = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        verifyOfType(mqttMsg, MqttMessageType.PUBLISH);
        verifySubscriptionIdentifier(123, (MqttPublishMessage) mqttMsg);
    }

    private static void verifySubscriptionIdentifier(int expectedSubscriptionIdentifier, MqttPublishMessage publish) {
        MqttProperties.MqttProperty<Integer> subscriptionIdentifierProperty = publish.variableHeader()
            .properties()
            .getProperty(MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value());
        assertNotNull(subscriptionIdentifierProperty, "subscription identifier property must be present");
        assertEquals(expectedSubscriptionIdentifier, subscriptionIdentifierProperty.value());
    }
}

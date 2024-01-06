package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import org.junit.jupiter.api.Test;

public class SubscriptionWithIdentifierTest extends AbstractSubscriptionIntegrationTest {

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenNonSharedSubscriptionWithIdentifierWhenPublishMatchedThenReceivesTheOriginalIdentifier() {
        final Mqtt5BlockingClient subscriberClient = createSubscriberClient();
        subscriberClient.subscribeWith()
            .topicFilter("$share/collectors/metric/temperature/#")
            .send();
    }
}

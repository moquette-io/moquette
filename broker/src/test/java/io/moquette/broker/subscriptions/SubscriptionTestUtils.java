package io.moquette.broker.subscriptions;

import org.jetbrains.annotations.NotNull;

import static io.moquette.broker.subscriptions.Topic.asTopic;

public class SubscriptionTestUtils {
    @NotNull
    static Subscription asSubscription(String clientId, String topicFilter) {
        return new Subscription(clientId, asTopic(topicFilter), null);
    }
}

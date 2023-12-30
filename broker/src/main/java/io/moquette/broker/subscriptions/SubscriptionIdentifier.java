package io.moquette.broker.subscriptions;

/**
 * Models the subscription identifier for MQTT5 Subscription.
 * */
public final class SubscriptionIdentifier {
    private final int subscriptionId;

    public SubscriptionIdentifier(int value) {
        if (value <= 0 || value > 268435455) {
            throw new IllegalArgumentException("Value MUST be > 0 and <= 268435455");
        }
        subscriptionId = value;
    }

    public int value() {
        return subscriptionId;
    }


}

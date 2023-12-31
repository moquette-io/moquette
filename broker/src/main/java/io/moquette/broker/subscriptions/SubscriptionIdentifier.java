package io.moquette.broker.subscriptions;

import java.util.Objects;

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

    @Override
    public String toString() {
        return "SubscriptionIdentifier: " + subscriptionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriptionIdentifier that = (SubscriptionIdentifier) o;
        return subscriptionId == that.subscriptionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionId);
    }
}

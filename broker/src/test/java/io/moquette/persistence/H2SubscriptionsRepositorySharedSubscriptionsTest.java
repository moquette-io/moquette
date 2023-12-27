package io.moquette.persistence;

import io.moquette.broker.subscriptions.ShareName;
import io.moquette.broker.subscriptions.SharedSubscription;
import io.moquette.broker.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

class H2SubscriptionsRepositorySharedSubscriptionsTest extends H2BaseTest {

    private H2SubscriptionsRepository sut;

    @BeforeEach
    public void setUp() {
        super.setUp();
        sut = new H2SubscriptionsRepository(mvStore);
    }

    @Test
    public void givenAPersistedSharedSubscriptionWhenListedThenItAppears() {
        sut.addNewSharedSubscription("subscriber", new ShareName("thermometers"),
            Topic.asTopic("/first_floor/living/temp"), MqttQoS.AT_MOST_ONCE);

        Collection<SharedSubscription> subscriptions = sut.listAllSharedSubscription();
        assertThat(subscriptions).hasSize(1);
        SharedSubscription subscription = subscriptions.iterator().next();
        Assertions.assertAll("First subscription match the previously stored",
            () -> Assertions.assertEquals("subscriber", subscription.clientId()),
            () -> Assertions.assertEquals("thermometers", subscription.getShareName().getShareName()));
    }

    @Test
    public void givenAPersistedSubscriptionWhenItsDeletedThenItNotAnymoreListed() {
        sut.addNewSharedSubscription("subscriber", new ShareName("thermometers"),
            Topic.asTopic("/first_floor/living/temp"), MqttQoS.AT_MOST_ONCE);
        assertThat(sut.listAllSharedSubscription()).hasSize(1);

        // remove the shared subscription
        sut.removeSharedSubscription("subscriber", new ShareName("thermometers"),
            Topic.asTopic("/first_floor/living/temp"));

        // verify it's not listed
        assertThat(sut.listAllSharedSubscription()).isEmpty();
    }

    @Test
    public void givenMultipleSharedSubscriptionForSameClientIdWhenTheyAreRemovedInBlockThenArentAnymoreListed() {
        String clientId = "subscriber";
        sut.addNewSharedSubscription(clientId, new ShareName("thermometers"),
            Topic.asTopic("/first_floor/living/temp"), MqttQoS.AT_MOST_ONCE);
        sut.addNewSharedSubscription(clientId, new ShareName("anemometers"),
            Topic.asTopic("/garden/wind/speed"), MqttQoS.AT_MOST_ONCE);
        sut.addNewSharedSubscription(clientId, new ShareName("anemometers"),
            Topic.asTopic("/garden/wind/direction"), MqttQoS.AT_MOST_ONCE);
        assertThat(sut.listAllSharedSubscription()).hasSize(3);

        // remove all shared subscriptions for client
        sut.removeAllSharedSubscriptions(clientId);

        // verify no shared subscriptions is listed
        assertThat(sut.listAllSharedSubscription()).isEmpty();
    }
}

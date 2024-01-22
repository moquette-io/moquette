package io.moquette.persistence;

import io.moquette.broker.subscriptions.ShareName;
import io.moquette.broker.subscriptions.SharedSubscription;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.SubscriptionIdentifier;
import io.moquette.broker.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class H2SubscriptionsRepositorySharedSubscriptionsTest extends H2BaseTest {

    private H2SubscriptionsRepository sut;

    @BeforeEach
    public void setUp() {
        super.setUp();
        sut = new H2SubscriptionsRepository(mvStore);
    }

    @Test
    public void givenNewSubscriptionWhenItsStoredThenCanGetRetrieved() {
        Subscription subscription = new Subscription("subscriber", Topic.asTopic("metering/temperature"),
            MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE), new SubscriptionIdentifier(1));
        sut.addNewSubscription(subscription);

        // verify deserialize
        Set<Subscription> subs = sut.listAllSubscriptions();
        assertThat(subs).hasSize(1);
        Subscription reloadedSub = subs.iterator().next();
        assertEquals(1, reloadedSub.getSubscriptionIdentifier().value());
    }

    @Test
    public void givenNewSharedSubscriptionWhenItsStoredThenCanGetRetrieved() {
        MqttSubscriptionOption option = MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE);
        sut.addNewSharedSubscription("subscriber", new ShareName("thermometers"),
            Topic.asTopic("/first_floor/living/temp"), option, new SubscriptionIdentifier(1));

        // verify deserialize
        Collection<SharedSubscription> subs = sut.listAllSharedSubscription();
        assertThat(subs).hasSize(1);
        SharedSubscription reloadedSub = subs.iterator().next();
        assertTrue(reloadedSub.hasSubscriptionIdentifier());
        assertEquals(1, reloadedSub.getSubscriptionIdentifier().value());
    }

    @Test
    public void givenAPersistedSharedSubscriptionWhenListedThenItAppears() {
        MqttSubscriptionOption op = MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE);
        sut.addNewSharedSubscription("subscriber", new ShareName("thermometers"),
            Topic.asTopic("/first_floor/living/temp"), op);

        Collection<SharedSubscription> subscriptions = sut.listAllSharedSubscription();
        assertThat(subscriptions).hasSize(1);
        SharedSubscription subscription = subscriptions.iterator().next();
        Assertions.assertAll("First subscription match the previously stored",
            () -> assertEquals("subscriber", subscription.clientId()),
            () -> assertEquals("thermometers", subscription.getShareName().getShareName()));
    }

    @Test
    public void givenAPersistedSubscriptionWhenItsDeletedThenItNotAnymoreListed() {
        MqttSubscriptionOption option = MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE);
        sut.addNewSharedSubscription("subscriber", new ShareName("thermometers"),
            Topic.asTopic("/first_floor/living/temp"), option);
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
        MqttSubscriptionOption atMostOnceOption = MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE);
        sut.addNewSharedSubscription(clientId, new ShareName("thermometers"),
            Topic.asTopic("/first_floor/living/temp"), atMostOnceOption);
        sut.addNewSharedSubscription(clientId, new ShareName("anemometers"),
            Topic.asTopic("/garden/wind/speed"), atMostOnceOption);
        sut.addNewSharedSubscription(clientId, new ShareName("anemometers"),
            Topic.asTopic("/garden/wind/direction"), atMostOnceOption);
        assertThat(sut.listAllSharedSubscription()).hasSize(3);

        // remove all shared subscriptions for client
        sut.removeAllSharedSubscriptions(clientId);

        // verify no shared subscriptions is listed
        assertThat(sut.listAllSharedSubscription()).isEmpty();
    }
}

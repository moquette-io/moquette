/*
 * Copyright (c) 2012-2023 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.moquette.broker.subscriptions;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import org.junit.jupiter.api.Test;
import java.util.List;
import static io.moquette.broker.subscriptions.Topic.asTopic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CTrieSharedSubscriptionDirectoryMatchingTest extends CTrieSubscriptionDirectMatchingCommon {

    static MqttSubscriptionOption asOption(MqttQoS qos) {
        return MqttSubscriptionOption.onlyFromQos(qos);
    }

    @Test
    public void whenNotMatchingSharedTopicThenNoSubscriptionShouldBeSelected() {
        sut.addShared("TempSensor1", new ShareName("temp_sensors"), asTopic("/"), asOption(MqttQoS.AT_MOST_ONCE));
        assertThat(sut.matchWithoutQosSharpening(asTopic("livingroom"))).isEmpty();

        sut.addShared("TempSensor1", new ShareName("temp_sensors"), asTopic("/livingroom"), asOption(MqttQoS.AT_MOST_ONCE));
        assertThat(sut.matchWithoutQosSharpening(asTopic("livingroom"))).isEmpty();
    }

    @Test
    public void whenMatchingSharedTopicThenOneSubscriptionShouldBeSelected() {
        sut.addShared("TempSensor1", new ShareName("temp_sensors"), asTopic("/livingroom"), asOption(MqttQoS.AT_MOST_ONCE));

        assertThat(sut.matchWithoutQosSharpening(asTopic("/livingroom")))
            .contains(SubscriptionTestUtils.asSubscription("TempSensor1", "/livingroom", "temp_sensors"));
    }

    @Test
    public void whenManySharedSubscriptionsOfDifferentShareNameMatchATopicThenOneSubscriptionForEachShareNameMustBeSelected() {
        sut.addShared("TempSensor1", new ShareName("temp_sensors"), asTopic("/livingroom"), asOption(MqttQoS.AT_MOST_ONCE));
        sut.addShared("TempSensor1", new ShareName("livingroom_devices"), asTopic("/livingroom"), asOption(MqttQoS.AT_MOST_ONCE));

        List<Subscription> matchingSubscriptions = sut.matchWithoutQosSharpening(asTopic("/livingroom"));
        assertThat(matchingSubscriptions)
            .containsOnly(SubscriptionTestUtils.asSubscription("TempSensor1", "/livingroom", "temp_sensors"),
                SubscriptionTestUtils.asSubscription("TempSensor1", "/livingroom", "livingroom_devices"))
            .as("One shared subscription for each share name must be present");
    }

    @Test
    public void givenSessionHasMultipleSharedSubscriptionWhenTheClientIsRemovedThenNoMatchingShouldHappen() {
        String clientId = "TempSensor1";
        sut.addShared(clientId, new ShareName("temp_sensors"), asTopic("/livingroom"), asOption(MqttQoS.AT_MOST_ONCE));
        sut.addShared(clientId, new ShareName("livingroom_devices"), asTopic("/livingroom"), asOption(MqttQoS.AT_MOST_ONCE));

        // Exercise
        sut.removeSharedSubscriptionsForClient(clientId);

        // Verify
        List<Subscription> matchingSubscriptions = sut.matchWithoutQosSharpening(asTopic("/livingroom"));
        assertThat(matchingSubscriptions).isEmpty();
    }

    @Test
    public void givenSubscriptionWithSubscriptionIdWhenNewSubscriptionIsProcessedThenSubscriptionIdIsUpdated() {
        // subscribe a client on topic with subscription identifier
        sut.addShared("client", new ShareName("share_temp"), asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE),
            new SubscriptionIdentifier(1));

        // verify it contains the subscription identifier
        final List<Subscription> matchingSubscriptions = sut.matchQosSharpening(asTopic("client/test/b"));
        verifySubscriptionIdentifierIsPresent(matchingSubscriptions, new SubscriptionIdentifier(1), "share_temp");

        // update the subscription of same clientId on same topic filter but with different subscription identifier
        sut.addShared("client", new ShareName("share_temp"), asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE),
            new SubscriptionIdentifier(123));

        // verify the subscription identifier is updated
        final List<Subscription> reloadedSubscriptions = sut.matchQosSharpening(asTopic("client/test/b"));
        verifySubscriptionIdentifierIsPresent(reloadedSubscriptions, new SubscriptionIdentifier(123), "share_temp");
    }

    private static void verifySubscriptionIdentifierIsPresent(List<Subscription> matchingSubscriptions, SubscriptionIdentifier subscriptionIdentifier, String expectedShareName) {
        assertAll("subscription contains the subscription identifier",
            () -> assertEquals(1, matchingSubscriptions.size()),
            () -> assertEquals(expectedShareName, matchingSubscriptions.iterator().next().shareName),
            () -> assertTrue(matchingSubscriptions.iterator().next().hasSubscriptionIdentifier()),
            () -> assertEquals(subscriptionIdentifier, matchingSubscriptions.iterator().next().getSubscriptionIdentifier())
        );
    }

    @Test
    public void givenSubscriptionWithSubscriptionIdWhenNewSubscriptionWithoutSubscriptionIdIsProcessedThenSubscriptionIdIsWiped() {
        // subscribe a client on topic with subscription identifier
        sut.addShared("client", new ShareName("share_temp"), asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE),
            new SubscriptionIdentifier(1));

        // verify it contains the subscription identifier
        SubscriptionIdentifier expectedSubscriptionId = new SubscriptionIdentifier(1);
        verifySubscriptionIdentifierIsPresent(sut.matchQosSharpening(asTopic("client/test/b")), expectedSubscriptionId, "share_temp");

        // update the subscription of same clientId on same topic filter but removing subscription identifier
        sut.addShared("client", new ShareName("share_temp"), asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE));

        // verify the subscription identifier is removed
        final List<Subscription> reloadedSubscriptions = sut.matchQosSharpening(asTopic("client/test/b"));
        assertAll("subscription doesn't contain subscription identifier",
            () -> assertEquals(1, reloadedSubscriptions.size()),
            () -> assertFalse(reloadedSubscriptions.iterator().next().hasSubscriptionIdentifier())
        );
    }
}

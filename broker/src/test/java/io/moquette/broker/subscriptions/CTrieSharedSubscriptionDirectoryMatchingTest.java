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
import org.junit.jupiter.api.Test;
import java.util.List;
import static io.moquette.broker.subscriptions.Topic.asTopic;
import static org.assertj.core.api.Assertions.assertThat;

public class CTrieSharedSubscriptionDirectoryMatchingTest extends CTrieSubscriptionDirectMatchingCommon {

    @Test
    public void whenNotMatchingSharedTopicThenNoSubscriptionShouldBeSelected() {
        sut.addShared("TempSensor1", new ShareName("temp_sensors"), asTopic("/"), MqttQoS.AT_MOST_ONCE);
        assertThat(sut.matchWithoutQosSharpening(asTopic("livingroom"))).isEmpty();

        sut.addShared("TempSensor1", new ShareName("temp_sensors"), asTopic("/livingroom"), MqttQoS.AT_MOST_ONCE);
        assertThat(sut.matchWithoutQosSharpening(asTopic("livingroom"))).isEmpty();
    }

    @Test
    public void whenMatchingSharedTopicThenOneSubscriptionShouldBeSelected() {
        sut.addShared("TempSensor1", new ShareName("temp_sensors"), asTopic("/livingroom"), MqttQoS.AT_MOST_ONCE);

        assertThat(sut.matchWithoutQosSharpening(asTopic("/livingroom")))
            .contains(SubscriptionTestUtils.asSubscription("TempSensor1", "/livingroom", "temp_sensors"));
    }

    @Test
    public void whenManySharedSubscriptionsOfDifferentShareNameMatchATopicThenOneSubscriptionForEachShareNameMustBeSelected() {
        sut.addShared("TempSensor1", new ShareName("temp_sensors"), asTopic("/livingroom"), MqttQoS.AT_MOST_ONCE);
        sut.addShared("TempSensor1", new ShareName("livingroom_devices"), asTopic("/livingroom"), MqttQoS.AT_MOST_ONCE);

        List<Subscription> matchingSubscriptions = sut.matchWithoutQosSharpening(asTopic("/livingroom"));
        assertThat(matchingSubscriptions)
            .containsOnly(SubscriptionTestUtils.asSubscription("TempSensor1", "/livingroom", "temp_sensors"),
                SubscriptionTestUtils.asSubscription("TempSensor1", "/livingroom", "livingroom_devices"))
            .as("One shared subscription for each share name must be present");
    }

    @Test
    public void givenSessionHasMultipleSharedSubscriptionWhenTheClientIsRemovedThenNoMatchingShouldHappen() {
        String clientId = "TempSensor1";
        sut.addShared(clientId, new ShareName("temp_sensors"), asTopic("/livingroom"), MqttQoS.AT_MOST_ONCE);
        sut.addShared(clientId, new ShareName("livingroom_devices"), asTopic("/livingroom"), MqttQoS.AT_MOST_ONCE);

        // Exercise
        sut.removeSharedSubscriptionsForClient(clientId);

        // Verify
        List<Subscription> matchingSubscriptions = sut.matchWithoutQosSharpening(asTopic("/livingroom"));
        assertThat(matchingSubscriptions).isEmpty();
    }
}

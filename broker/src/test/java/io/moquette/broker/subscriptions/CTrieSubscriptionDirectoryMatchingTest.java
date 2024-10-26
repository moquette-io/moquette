/*
 * Copyright (c) 2012-2018 The original author or authors
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

import io.moquette.broker.ISubscriptionsRepository;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.moquette.broker.subscriptions.CTrieSharedSubscriptionDirectoryMatchingTest.asOption;
import static io.moquette.broker.subscriptions.Topic.asTopic;
import java.util.ArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class CTrieSubscriptionDirectoryMatchingTest extends CTrieSubscriptionDirectMatchingCommon {

    @Test
    public void testMatchSimple() {
        sut.add("TempSensor1", asTopic("/"), null);
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance"))).isEmpty();

        sut.add("TempSensor1", asTopic("/finance"), null);
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance"))).isEmpty();

        assertThat(sut.matchWithoutQosSharpening(asTopic("/finance"))).contains(SubscriptionTestUtils.asSubscription("TempSensor1", "/finance"));
        assertThat(sut.matchWithoutQosSharpening(asTopic("/"))).contains(SubscriptionTestUtils.asSubscription("TempSensor1", "/"));
    }

    @Test
    public void testMatchingDeepMulti_one_layer() {
        sut.add("AllSensor1", asTopic("#"), null);
        sut.add("FinanceSensor", asTopic("finance/#"), null);

        // Verify
        Subscription anySub = SubscriptionTestUtils.asSubscription("AllSensor1", "#");
        Subscription financeAnySub = SubscriptionTestUtils.asSubscription("FinanceSensor", "finance/#");
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance/stock")))
            .containsExactlyInAnyOrder(financeAnySub, anySub);
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance/stock/ibm")))
            .containsExactlyInAnyOrder(financeAnySub, anySub);
    }

    @Test
    public void testMatchSimpleMulti() {
        Subscription anySub = SubscriptionTestUtils.asSubscription("TempSensor1", "#");
        sut.add("TempSensor1", asTopic("#"), null);
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance"))).contains(anySub);

        Subscription financeAnySub = SubscriptionTestUtils.asSubscription("TempSensor1", "finance/#");
        sut.add("TempSensor1", asTopic("finance/#"), null);
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance"))).containsExactlyInAnyOrder(financeAnySub, anySub);
    }

    @Test
    public void testMatchingDeepMulti_two_layer() {
        sut.add("FinanceSensor", asTopic("finance/stock/#"), null);

        // Verify
        Subscription financeAnySub = SubscriptionTestUtils.asSubscription("FinanceSensor", "finance/stock/#");
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance/stock/ibm"))).containsExactly(financeAnySub);
    }

    @Test
    public void testMatchSimpleSingle() {
        Subscription anySub = SubscriptionTestUtils.asSubscription("AnySensor", "+");
        sut.add("AnySensor", asTopic("+"), null);
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance"))).containsExactly(anySub);

        Subscription financeOne = SubscriptionTestUtils.asSubscription("AnySensor", "finance/+");
        sut.add("AnySensor", asTopic("finance/+"), null);
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance/stock"))).containsExactly(financeOne);
    }

    @Test
    public void testMatchManySingle() {
        sut.add("AnySensor", asTopic("+/+"), null);

        // verify
        Subscription manySub = SubscriptionTestUtils.asSubscription("AnySensor", "+/+");
        assertThat(sut.matchWithoutQosSharpening(asTopic("/finance"))).contains(manySub);
    }

    @Test
    public void testMatchSlashSingle() {
        sut.add("AnySensor", asTopic("/+"), null);
        sut.add("AnySensor", asTopic("+"), null);

        // Verify
        Subscription slashPlusSub = SubscriptionTestUtils.asSubscription("AnySensor", "/+");
        Subscription anySub = SubscriptionTestUtils.asSubscription("AnySensor", "+");
        assertThat(sut.matchWithoutQosSharpening(asTopic("/finance"))).containsOnly(slashPlusSub);
        assertThat(sut.matchWithoutQosSharpening(asTopic("/finance"))).doesNotContain(anySub);
    }

    @Test
    public void testMatchManyDeepSingle() {
        sut.add("FinanceSensor1", asTopic("/finance/+/ibm"), null);
        sut.add("FinanceSensor2", asTopic("/+/stock/+"), null);

        // Verify
        Subscription slashPlusSub = SubscriptionTestUtils.asSubscription("FinanceSensor1", "/finance/+/ibm");
        Subscription slashPlusDeepSub = SubscriptionTestUtils.asSubscription("FinanceSensor2", "/+/stock/+");
        assertThat(sut.matchWithoutQosSharpening(asTopic("/finance/stock/ibm")))
            .containsExactlyInAnyOrder(slashPlusSub, slashPlusDeepSub);
    }

    @Test
    public void testMatchSimpleMulti_allTheTree() {
        sut.add("AnySensor1", asTopic("#"), null);

        assertThat(sut.matchWithoutQosSharpening(asTopic("finance"))).isNotEmpty();
        assertThat(sut.matchWithoutQosSharpening(asTopic("finance/ibm"))).isNotEmpty();
    }

    @Test
    public void rogerLightTopicMatches() {
        assertMatch("foo/bar", "foo/bar");
        assertMatch("foo/bar", "foo/bar");
        assertMatch("foo/+", "foo/bar");
        assertMatch("foo/+/baz", "foo/bar/baz");
        assertMatch("foo/+/#", "foo/bar/baz");
        assertMatch("#", "foo/bar/baz");

        assertNotMatch("foo/bar", "foo");
        assertNotMatch("foo/+", "foo/bar/baz");
        assertNotMatch("foo/+/baz", "foo/bar/bar");
        assertNotMatch("foo/+/#", "fo2/bar/baz");

        assertMatch("#", "/foo/bar");
        assertMatch("/#", "/foo/bar");
        assertNotMatch("/#", "foo/bar");

        assertMatch("foo//bar", "foo//bar");
        assertMatch("foo//+", "foo//bar");
        assertMatch("foo/+/+/baz", "foo///baz");
        assertMatch("foo/bar/+", "foo/bar/");
    }

    @Test
    public void givenTopicFilterStartingWithSingleWildcardDoesntMatchSpecialTopicNames() {
        assertNotMatch("+/monitor/clients", "$SYS/monitor/clients");
        assertMatch("outer/+/inner", "outer/$something/inner");
        assertMatch("$SYS/monitor/+", "$SYS/monitor/clients");
    }

    @Test
    public void givenTopicFilterStartingWithMultiWildcardDoesntMatchSpecialTopicNames() {
        assertNotMatch("#", "$SYS/monitor/clients");
        assertMatch("$SYS/#", "$SYS");
    }

    private void assertMatch(String topicFilter, String topicName) {
        sut = new CTrieSubscriptionDirectory();
        ISubscriptionsRepository sessionsRepository = new MemorySubscriptionsRepository();
        sut.init(sessionsRepository);

        sut.add("AnySensor1", asTopic(topicFilter), null);

        assertThat(sut.matchWithoutQosSharpening(asTopic(topicName))).isNotEmpty();
    }

    private void assertNotMatch(String topicFilter, String topicName) {
        sut = new CTrieSubscriptionDirectory();
        ISubscriptionsRepository sessionsRepository = new MemorySubscriptionsRepository();
        sut.init(sessionsRepository);

        sut.add("AnySensor1", asTopic(topicFilter), null);

        assertThat(sut.matchWithoutQosSharpening(asTopic(topicName))).isEmpty();
    }

    @Test
    public void testOverlappingSubscriptions() {
        Subscription genericSub = new Subscription("Sensor1", asTopic("a/+"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        this.sessionsRepository.addNewSubscription(genericSub);
        sut.add(genericSub.clientId, genericSub.topicFilter, genericSub.option());

        Subscription specificSub = new Subscription("Sensor1", asTopic("a/b"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        this.sessionsRepository.addNewSubscription(specificSub);
        sut.add(specificSub.clientId, specificSub.topicFilter, specificSub.option());

        //Exercise
        final SubscriptionCollection matchingForSpecific = sut.matchWithoutQosSharpening(asTopic("a/b"));

        // Verify
        assertThat(matchingForSpecific.size()).isEqualTo(1);
    }

    @Test
    public void removeSubscription_withDifferentClients_subscribedSameTopic() {
        sut.add("Sensor1", asTopic("/topic"), null);
        sut.add("Sensor2", asTopic("/topic"), null);

        // Exercise
        sut.removeSubscription(asTopic("/topic"), "Sensor2");

        // Verify
        Subscription remainedSubscription = sut.matchWithoutQosSharpening(asTopic("/topic")).iterator().next();
        assertThat(remainedSubscription.clientId).isEqualTo("Sensor1");
        assertEquals("Sensor1", remainedSubscription.clientId);
    }

    @Test
    public void removeSubscription_sameClients_subscribedSameTopic() {
        sut.add("Sensor1", asTopic("/topic"), null);

        // Exercise
        sut.removeSubscription(asTopic("/topic"), "Sensor1");

        // Verify
        final SubscriptionCollection matchingSubscriptions = sut.matchWithoutQosSharpening(asTopic("/topic"));
        assertThat(matchingSubscriptions).isEmpty();
    }

    /*
     * Test for Issue #49
     */
    @Test
    public void duplicatedSubscriptionsWithDifferentQos() {
        Subscription client2Sub = new Subscription("client2", asTopic("client/test/b"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        this.sut.add("client2", asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE));
        Subscription client1SubQoS0 = new Subscription("client1", asTopic("client/test/b"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        this.sut.add("client1", asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE));

        Subscription client1SubQoS2 = new Subscription("client1", asTopic("client/test/b"), MqttSubscriptionOption.onlyFromQos(MqttQoS.EXACTLY_ONCE));
        this.sut.add("client1", asTopic("client/test/b"), asOption(MqttQoS.EXACTLY_ONCE));

        // Verify
        SubscriptionCollection subscriptions = this.sut.matchWithoutQosSharpening(asTopic("client/test/b"));
        assertThat(subscriptions).contains(client1SubQoS2);
        assertThat(subscriptions).contains(client2Sub);

        Optional<Subscription> matchingClient1Sub = Optional.empty();
        for (Subscription sub : subscriptions) {
            if (sub.equals(client1SubQoS0)) {
                matchingClient1Sub = Optional.of(sub);
                break;
            }
        }
        assertTrue(matchingClient1Sub.isPresent());
        Subscription client1Sub = matchingClient1Sub.get();

        assertThat(client1SubQoS0.option().qos()).isNotEqualTo(client1Sub.option().qos());

        // client1SubQoS2 should override client1SubQoS0
        assertThat(client1Sub.option().qos()).isEqualTo(client1SubQoS2.option().qos());
    }

    @Test
    public void givenSubscriptionWithSubscriptionIdWhenNewSubscriptionIsProcessedThenSubscriptionIdIsUpdated() {
        // subscribe a client on topic with subscription identifier
        sut.add("client", asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE), new SubscriptionIdentifier(1));

        // verify it contains the subscription identifier
        final SubscriptionCollection matchingSubscriptions = sut.matchWithoutQosSharpening(asTopic("client/test/b"));
        verifySubscriptionIdentifierIsPresent(matchingSubscriptions, new SubscriptionIdentifier(1));

        // update the subscription of same clientId on same topic filter but with different subscription identifier
        sut.add("client", asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE), new SubscriptionIdentifier(123));

        // verify the subscription identifier is updated
        final SubscriptionCollection reloadedSubscriptions = sut.matchWithoutQosSharpening(asTopic("client/test/b"));
        verifySubscriptionIdentifierIsPresent(reloadedSubscriptions, new SubscriptionIdentifier(123));
    }

    private static void verifySubscriptionIdentifierIsPresent(SubscriptionCollection matchingSubscriptions, SubscriptionIdentifier subscriptionIdentifier) {
        assertAll("subscription contains the subscription identifier",
            () -> assertEquals(1, matchingSubscriptions.size()),
            () -> assertTrue(matchingSubscriptions.iterator().next().hasSubscriptionIdentifier()),
            () -> assertEquals(subscriptionIdentifier, matchingSubscriptions.iterator().next().getSubscriptionIdentifier())
        );
    }

    @Test
    public void givenSubscriptionWithSubscriptionIdWhenNewSubscriptionWithoutSubscriptionIdIsProcessedThenSubscriptionIdIsWiped() {
        // subscribe a client on topic with subscription identifier
        sut.add("client", asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE), new SubscriptionIdentifier(1));

        // verify it contains the subscription identifier
        SubscriptionIdentifier expectedSubscriptionId = new SubscriptionIdentifier(1);
        verifySubscriptionIdentifierIsPresent(sut.matchWithoutQosSharpening(asTopic("client/test/b")), expectedSubscriptionId);

        // update the subscription of same clientId on same topic filter but removing subscription identifier
        sut.add("client", asTopic("client/test/b"), asOption(MqttQoS.AT_MOST_ONCE));

        // verify the subscription identifier is removed
        final SubscriptionCollection reloadedSubscriptions = sut.matchWithoutQosSharpening(asTopic("client/test/b"));
        assertAll("subscription doesn't contain subscription identifier",
            () -> assertEquals(1, reloadedSubscriptions.size()),
            () -> assertFalse(reloadedSubscriptions.iterator().next().hasSubscriptionIdentifier())
        );
    }
}

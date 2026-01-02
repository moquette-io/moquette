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


import io.moquette.broker.subscriptions.CTrie.SubscriptionRequest;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.moquette.broker.subscriptions.SubscriptionTestUtils.asSubscription;
import static io.moquette.broker.subscriptions.Topic.asTopic;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CTrieTest {

    private CTrie sut;

    @BeforeEach
    public void setUp() {
        sut = new CTrie();
    }

    @Test
    public void testAddOnSecondLayerWithEmptyTokenOnEmptyTree() {
        //Exercise
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "/");
        sut.addToTree(newSubscription);

        //Verify
        final Optional<CNode> matchedNode = sut.lookup(asTopic("/"));
        assertTrue(matchedNode.isPresent(), "Node on path / must be present");
        //verify structure, only root INode and the first CNode should be present
        assertThat(this.sut.root.mainNode().subscriptions()).isEmpty();
        assertThat(this.sut.root.mainNode().allChildren()).isNotEmpty();

        INode firstLayer = this.sut.root.mainNode().allChildren().get(0);
        assertThat(firstLayer.mainNode().subscriptions()).isEmpty();
        assertThat(firstLayer.mainNode().allChildren()).isNotEmpty();

        INode secondLayer = firstLayer.mainNode().allChildren().get(0);
        assertThat(secondLayer.mainNode().subscriptions()).isNotEmpty();
        assertThat(secondLayer.mainNode().allChildren()).isEmpty();
    }

    @Test
    public void testAddFirstLayerNodeOnEmptyTree() {
        //Exercise
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "/temp");
        sut.addToTree(newSubscription);

        //Verify
        final Optional<CNode> matchedNode = sut.lookup(asTopic("/temp"));
        assertTrue(matchedNode.isPresent(), "Node on path /temp must be present");
        assertFalse(matchedNode.get().subscriptions().isEmpty());
    }

    @Test
    public void testLookup() {
        final SubscriptionRequest existingSubscription = clientSubOnTopic("TempSensor1", "/temp");
        sut.addToTree(existingSubscription);

        //Exercise
        final Optional<CNode> matchedNode = sut.lookup(asTopic("/humidity"));

        //Verify
        assertFalse(matchedNode.isPresent(), "Node on path /humidity can't be present");
    }

    @Test
    public void testAddNewSubscriptionOnExistingNode() {
        final SubscriptionRequest existingSubscription = clientSubOnTopic("TempSensor1", "/temp");
        assertTrue(sut.addToTree(existingSubscription), "First created subscription on topic filter MUST return true");

        //Exercise
        final SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor2", "/temp");
        assertFalse(sut.addToTree(newSubscription), "Not new created subscription on topic filter MUST return false");

        //Verify
        final Optional<CNode> matchedNode = sut.lookup(asTopic("/temp"));
        assertTrue(matchedNode.isPresent(), "Node on path /temp must be present");
        final List<Subscription> subscriptions = matchedNode.get().subscriptions();
        assertTrue(subscriptions.contains(asSubscription("TempSensor2", "/temp")));
    }

    @Test
    public void testAddNewDeepNodes() {
        SubscriptionRequest newSubscription2 = clientSubOnTopic("TempSensorRM", "/italy/roma/temp");
        sut.addToTree(newSubscription2);
        SubscriptionRequest newSubscription1 = clientSubOnTopic("TempSensorFI", "/italy/firenze/temp");
        sut.addToTree(newSubscription1);
        SubscriptionRequest newSubscription = clientSubOnTopic("HumSensorFI", "/italy/roma/humidity");
        sut.addToTree(newSubscription);
        final SubscriptionRequest happinessSensor = clientSubOnTopic("HappinessSensor", "/italy/happiness");
        sut.addToTree(happinessSensor);

        //Verify
        final Optional<CNode> matchedNode = sut.lookup(asTopic("/italy/happiness"));
        assertTrue(matchedNode.isPresent(), "Node on path /italy/happiness must be present");
        final List<Subscription> subscriptions = matchedNode.get().subscriptions();
        assertTrue(subscriptions.contains(asSubscription("HappinessSensor", "/italy/happiness")));
    }

    static SubscriptionRequest clientSubOnTopic(String clientID, String topicFilter) {
        return SubscriptionRequest.buildNonShared(clientID, asTopic(topicFilter), MqttSubscriptionOption.onlyFromQos(null));
    }

    @Test
    public void givenTreeWithSomeNodeWhenRemoveContainedSubscriptionThenNodeIsUpdated() {
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "/temp");
        sut.addToTree(newSubscription);

        //Exercise
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("/temp")));

        //Verify
        final Optional<CNode> matchedNode = sut.lookup(asTopic("/temp"));
        assertFalse(matchedNode.isPresent(), "Node on path /temp can't be present");
    }

    @Test
    public void givenTreeWithSomeNodeUnsubscribeAndResubscribeCleanTomb() {
        SubscriptionRequest newSubscription1 = clientSubOnTopic("TempSensor1", "test");
        sut.addToTree(newSubscription1);
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("test")));

        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "test");
        sut.addToTree(newSubscription);
        assertEquals(1, sut.root.mainNode().allChildren().size());  // looking to see if TNode is cleaned up
    }

    @Test
    public void givenTreeWithSomeNodeWhenRemoveMultipleTimes() {
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "test");
        sut.addToTree(newSubscription);

        // make sure no TNode exceptions
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("test")));
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("test")));
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("test")));
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("test")));

        //Verify
        final Optional<CNode> matchedNode = sut.lookup(asTopic("/temp"));
        assertFalse(matchedNode.isPresent(), "Node on path /temp can't be present");
    }

    @Test
    public void givenTreeWithSomeDeepNodeWhenRemoveMultipleTimes() {
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "/test/me/1/2/3");
        sut.addToTree(newSubscription);

        // make sure no TNode exceptions
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("/test/me/1/2/3")));
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("/test/me/1/2/3")));
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("/test/me/1/2/3")));

        //Verify
        final Optional<CNode> matchedNode = sut.lookup(asTopic("/temp"));
        assertFalse(matchedNode.isPresent(), "Node on path /temp can't be present");
    }

    @Test
    public void givenTreeWithSomeNodeHierarchyWhenRemoveContainedSubscriptionThenNodeIsUpdated() {
        SubscriptionRequest newSubscription1 = clientSubOnTopic("TempSensor1", "/temp/1");
        sut.addToTree(newSubscription1);
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "/temp/2");
        sut.addToTree(newSubscription);

        //Exercise
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("/temp/1")));

        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("/temp/1")));
        final List<Subscription> matchingSubs = sut.recursiveMatch(asTopic("/temp/2"));

        //Verify
        final Subscription expectedMatchingsub = new Subscription("TempSensor1", asTopic("/temp/2"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        assertThat(matchingSubs).contains(expectedMatchingsub);
    }

    @Test
    public void givenTreeWithSomeNodeHierarchWhenRemoveContainedSubscriptionSmallerThenNodeIsNotUpdated() {
        SubscriptionRequest newSubscription1 = clientSubOnTopic("TempSensor1", "/temp/1");
        sut.addToTree(newSubscription1);
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "/temp/2");
        sut.addToTree(newSubscription);

        //Exercise
        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("/temp")));

        final List<Subscription> matchingSubs1 = sut.recursiveMatch(asTopic("/temp/1"));
        final List<Subscription> matchingSubs2 = sut.recursiveMatch(asTopic("/temp/2"));

        //Verify
        // not clear to me, but I believe /temp unsubscribe should not unsub you from downstream /temp/1 or /temp/2
        final Subscription expectedMatchingsub1 = new Subscription("TempSensor1", asTopic("/temp/1"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        assertThat(matchingSubs1).contains(expectedMatchingsub1);
        final Subscription expectedMatchingsub2 = new Subscription("TempSensor1", asTopic("/temp/2"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        assertThat(matchingSubs2).contains(expectedMatchingsub2);
    }

    @Test
    public void givenTreeWithDeepNodeWhenRemoveContainedSubscriptionThenNodeIsUpdated() {
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "/bah/bin/bash");
        sut.addToTree(newSubscription);

        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("/bah/bin/bash")));

        //Verify
        final Optional<CNode> matchedNode = sut.lookup(asTopic("/bah/bin/bash"));
        assertFalse(matchedNode.isPresent(), "Node on path /temp can't be present");
    }

    @Test
    public void testMatchSubscriptionNoWildcards() {
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "/temp");
        sut.addToTree(newSubscription);

        //Exercise
        final List<Subscription> matchingSubs = sut.recursiveMatch(asTopic("/temp"));

        //Verify
        final Subscription expectedMatchingsub = new Subscription("TempSensor1", asTopic("/temp"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        assertThat(matchingSubs).contains(expectedMatchingsub);
    }

    @Test
    public void testRemovalInnerTopicOffRootSameClient() {
        SubscriptionRequest newSubscription1 = clientSubOnTopic("TempSensor1", "temp");
        sut.addToTree(newSubscription1);
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor1", "temp/1");
        sut.addToTree(newSubscription);

        //Exercise
        final List<Subscription> matchingSubs1 = sut.recursiveMatch(asTopic("temp"));
        final List<Subscription> matchingSubs2 = sut.recursiveMatch(asTopic("temp/1"));

        //Verify
        final Subscription expectedMatchingsub1 = new Subscription("TempSensor1", asTopic("temp"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        final Subscription expectedMatchingsub2 = new Subscription("TempSensor1", asTopic("temp/1"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));

        assertThat(matchingSubs1).contains(expectedMatchingsub1);
        assertThat(matchingSubs2).contains(expectedMatchingsub2);

        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("temp")));

        //Exercise
        final List<Subscription> matchingSubs3 = sut.recursiveMatch(asTopic("temp"));
        final List<Subscription> matchingSubs4 = sut.recursiveMatch(asTopic("temp/1"));

        assertThat(matchingSubs3).doesNotContain(expectedMatchingsub1);
        assertThat(matchingSubs4).contains(expectedMatchingsub2);
    }

    @Test
    public void testRemovalInnerTopicOffRootDiffClient() {
        SubscriptionRequest newSubscription1 = clientSubOnTopic("TempSensor1", "temp");
        sut.addToTree(newSubscription1);
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor2", "temp/1");
        sut.addToTree(newSubscription);

        //Exercise
        final List<Subscription> matchingSubs1 = sut.recursiveMatch(asTopic("temp"));
        final List<Subscription> matchingSubs2 = sut.recursiveMatch(asTopic("temp/1"));

        //Verify
        final Subscription expectedMatchingsub1 = new Subscription("TempSensor1", asTopic("temp"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        final Subscription expectedMatchingsub2 = new Subscription("TempSensor2", asTopic("temp/1"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));

        assertThat(matchingSubs1).contains(expectedMatchingsub1);
        assertThat(matchingSubs2).contains(expectedMatchingsub2);

        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor1", asTopic("temp")));

        //Exercise
        final List<Subscription> matchingSubs3 = sut.recursiveMatch(asTopic("temp"));
        final List<Subscription> matchingSubs4 = sut.recursiveMatch(asTopic("temp/1"));

        assertThat(matchingSubs3).doesNotContain(expectedMatchingsub1);
        assertThat(matchingSubs4).contains(expectedMatchingsub2);
    }

    @Test
    public void testRemovalOuterTopicOffRootDiffClient() {
        SubscriptionRequest newSubscription1 = clientSubOnTopic("TempSensor1", "temp");
        sut.addToTree(newSubscription1);
        SubscriptionRequest newSubscription = clientSubOnTopic("TempSensor2", "temp/1");
        sut.addToTree(newSubscription);

        //Exercise
        final List<Subscription> matchingSubs1 = sut.recursiveMatch(asTopic("temp"));
        final List<Subscription> matchingSubs2 = sut.recursiveMatch(asTopic("temp/1"));

        //Verify
        final Subscription expectedMatchingsub1 = new Subscription("TempSensor1", asTopic("temp"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));
        final Subscription expectedMatchingsub2 = new Subscription("TempSensor2", asTopic("temp/1"), MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE));

        assertThat(matchingSubs1).contains(expectedMatchingsub1);
        assertThat(matchingSubs2).contains(expectedMatchingsub2);

        sut.removeFromTree(CTrie.UnsubscribeRequest.buildNonShared("TempSensor2", asTopic("temp/1")));

        //Exercise
        final List<Subscription> matchingSubs3 = sut.recursiveMatch(asTopic("temp"));
        final List<Subscription> matchingSubs4 = sut.recursiveMatch(asTopic("temp/1"));

        assertThat(matchingSubs3).contains(expectedMatchingsub1);
        assertThat(matchingSubs4).doesNotContain(expectedMatchingsub2);
    }
}

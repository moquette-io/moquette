/*
 * Copyright (c) 2012-2017 The original author or authors
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

package io.moquette.persistence;

import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISubscriptionsStore.ClientTopicCouple;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.Test;
import java.util.Collection;
import java.util.List;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.*;
import static io.netty.handler.codec.mqtt.MqttQoS.*;

public abstract class AbstractStoreTest extends MessageStoreTCK {

    @Test
    public void overridingSubscriptions() {
        ClientSession session1 = sessionsStore.createNewSession("SESSION_ID_1", true);

        // Subscribe on /topic with QOSType.MOST_ONE
        Subscription oldSubscription = new Subscription(session1.clientID, new Topic("/topic"), AT_MOST_ONCE);
        session1.subscribe(oldSubscription);

        // Subscribe on /topic again that overrides the previous subscription.
        Subscription overridingSubscription = new Subscription(session1.clientID, new Topic("/topic"), EXACTLY_ONCE);
        session1.subscribe(overridingSubscription);

        // Verify
        List<ClientTopicCouple> subscriptions = sessionsStore.subscriptionStore().listAllSubscriptions();
        assertThat(subscriptions).hasSize(1);
        Subscription sub = sessionsStore.subscriptionStore().getSubscription(subscriptions.get(0));
        assertThat(sub.getRequestedQos()).isEqualTo(overridingSubscription.getRequestedQos());
    }

    @Test(expected = RuntimeException.class)
    public void testNextPacketID_notExistingClientSession() {
         sessionsStore.nextPacketID("NOT_EXISTING_CLI");
    }

    @Test
    public void testNextPacketID_existingClientSession() {
        sessionsStore.createNewSession("CLIENT", true);

        // Force creation of inflight map for the CLIENT session
        int packetId = sessionsStore.nextPacketID("CLIENT");
        assertThat(packetId).isEqualTo(1);

        // request a second packetID
        packetId = sessionsStore.nextPacketID("CLIENT");
        assertThat(packetId).isEqualTo(2);
    }

    @Test
    public void testNextPacketID() {
        sessionsStore.createNewSession("CLIENT", true);
        StoredMessage msgStored = new StoredMessage("Hello".getBytes(), MqttQoS.AT_LEAST_ONCE, "/topic");
        msgStored.setClientID(TEST_CLIENT);

        // request a first ID

        int packetId = sessionsStore.nextPacketID("CLIENT");
        sessionsStore.inFlight("CLIENT", packetId, msgStored); // simulate an inflight
        assertThat(packetId).isEqualTo(1);

        // release the ID
        sessionsStore.inFlightAck("CLIENT", packetId);

        // request a second packetID, counter restarts from 0
        packetId = sessionsStore.nextPacketID("CLIENT");
        assertThat(packetId).isEqualTo(1);
    }

    @Test
    public void checkRetained() {
        final StoredMessage message = new StoredMessage("message".getBytes(), AT_MOST_ONCE, "id1/topic");
        message.setRetained(true);
        message.setClientID("id1");
        messagesStore.storeRetained(new Topic(message.getTopic()), message);

        final StoredMessage message2 = new StoredMessage("message".getBytes(), AT_MOST_ONCE, "id1/topic2");
        message2.setRetained(true);
        message2.setClientID("id1");
        messagesStore.storeRetained(new Topic(message2.getTopic()), message2);

        final StoredMessage message3 = new StoredMessage("message".getBytes(), AT_MOST_ONCE, "id2/topic2");
        message3.setRetained(true);
        message3.setClientID("id1");
        messagesStore.storeRetained(new Topic(message3.getTopic()), message3);

        final Subscription subscription1 = new Subscription("cid", new Topic("id1/#"), AT_MOST_ONCE);

        final Subscription subscription2 = new Subscription("cid", new Topic("id1/topic"), AT_MOST_ONCE);

        await().untilAsserted(() -> {
                Collection<StoredMessage> result = messagesStore.searchMatching(key ->
                    key.match(subscription1.getTopicFilter()) || key.match(subscription2.getTopicFilter()));

                assertThat(result).containsOnly(message, message2);
        });

        messagesStore.cleanRetained(new Topic("id1/topic2"));

        await().untilAsserted(() -> {
                Collection<StoredMessage> result = messagesStore.searchMatching(key ->
                    key.match(subscription1.getTopicFilter()) || key.match(subscription2.getTopicFilter()));

                assertThat(result).containsOnly(message);
        });

        messagesStore.cleanRetained(new Topic("id1/topic"));

        await().untilAsserted(() -> {
                Collection<StoredMessage> result = messagesStore.searchMatching(key ->
                    key.match(subscription1.getTopicFilter()) || key.match(subscription2.getTopicFilter()));

                assertThat(result).isEmpty();
        });
    }

    @Test
    public void singleTopicRetained() {
        // This message will be stored in a wrong place id1/
        final StoredMessage message = new StoredMessage("message".getBytes(), AT_MOST_ONCE, "id1");
        message.setRetained(true);
        message.setClientID("id1");
        messagesStore.storeRetained(new Topic(message.getTopic()), message);

        final Subscription subscription1 = new Subscription("cid", new Topic("id1/#"), AT_MOST_ONCE);

        final Subscription subscription2 = new Subscription("cid", new Topic("id1"), AT_MOST_ONCE);

        await().untilAsserted(() -> {
                Collection<StoredMessage> result = messagesStore.searchMatching(key ->
                        key.match(subscription1.getTopicFilter()) || key.match(subscription2.getTopicFilter()));

                assertThat(result).containsOnly(message);
        });

        messagesStore.cleanRetained(new Topic("id1"));
        await().untilAsserted(() -> {
                Collection<StoredMessage> result = messagesStore.searchMatching(key ->
                        key.match(subscription1.getTopicFilter()) || key.match(subscription2.getTopicFilter()));

                assertThat(result).isEmpty();
        });
    }
}

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

package io.moquette.spi.impl.subscriptions;

import io.moquette.persistence.MemoryStorageService;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.moquette.spi.ClientSession;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.ISubscriptionsStore;
import io.moquette.spi.ISubscriptionsStore.ClientTopicCouple;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SubscriptionsDirectoryTest {

    private SubscriptionsDirectory store;
    private ISessionsStore sessionsStore;
    private ISubscriptionsStore subscriptionsStore;

    public SubscriptionsDirectoryTest() {
    }

    @Before
    public void setUp() throws IOException {
        store = new SubscriptionsDirectory();
        IConfig config = new MemoryConfig(System.getProperties());
        MemoryStorageService storageService = new MemoryStorageService(config, null);
        this.sessionsStore = storageService.sessionsStore();
        storageService.initStore();
        store.init(sessionsStore);

        this.subscriptionsStore = this.sessionsStore.subscriptionStore();

        storageService.sessionsStore().createNewSession("FAKE_CLI_ID_1", false);
        storageService.sessionsStore().createNewSession("FAKE_CLI_ID_2", false);
    }

    @Test
    public void testMatchSimple() {
        Subscription slashSub = new Subscription("FAKE_CLI_ID_1", new Topic("/"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(slashSub);
        store.add(slashSub.asClientTopicCouple());
        assertThat(store.matches(new Topic("finance"))).isEmpty();

        Subscription slashFinanceSub = new Subscription("FAKE_CLI_ID_1", new Topic("/finance"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(slashFinanceSub);
        store.add(slashFinanceSub.asClientTopicCouple());
        assertThat(store.matches(new Topic("finance"))).isEmpty();

        assertThat(store.matches(new Topic("/finance"))).contains(slashFinanceSub);
        assertThat(store.matches(new Topic("/"))).contains(slashSub);
    }

    @Test
    public void testMatchSimpleMulti() {
        Subscription anySub = new Subscription("FAKE_CLI_ID_1", new Topic("#"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(anySub);
        store.add(anySub.asClientTopicCouple());
        assertThat(store.matches(new Topic("finance"))).contains(anySub);

        Subscription financeAnySub = new Subscription("FAKE_CLI_ID_2", new Topic("finance/#"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(financeAnySub);
        store.add(financeAnySub.asClientTopicCouple());
        assertThat(store.matches(new Topic("finance"))).containsExactlyInAnyOrder(financeAnySub, anySub);
    }

    @Test
    public void testMatchingDeepMulti_one_layer() {
        Subscription anySub = new Subscription("FAKE_CLI_ID_1", new Topic("#"), MqttQoS.AT_MOST_ONCE);
        Subscription financeAnySub = new Subscription("FAKE_CLI_ID_2", new Topic("finance/#"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(anySub);
        this.subscriptionsStore.addNewSubscription(financeAnySub);
        store.add(anySub.asClientTopicCouple());
        store.add(financeAnySub.asClientTopicCouple());

        // Verify
        assertThat(store.matches(new Topic("finance/stock"))).containsExactlyInAnyOrder(financeAnySub, anySub);
        assertThat(store.matches(new Topic("finance/stock/ibm"))).containsExactlyInAnyOrder(financeAnySub, anySub);
    }

    @Test
    public void testMatchingDeepMulti_two_layer() {
        Subscription financeAnySub = new Subscription(
                "FAKE_CLI_ID_1",
                new Topic("finance/stock/#"),
                MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(financeAnySub);
        store.add(financeAnySub.asClientTopicCouple());

        // Verify
        assertThat(store.matches(new Topic("finance/stock/ibm"))).containsExactly(financeAnySub);
    }

    @Test
    public void testMatchSimpleSingle() {
        Subscription anySub = new Subscription("FAKE_CLI_ID_1", new Topic("+"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(anySub);
        store.add(anySub.asClientTopicCouple());
        assertThat(store.matches(new Topic("finance"))).containsExactly(anySub);

        Subscription financeOne = new Subscription("FAKE_CLI_ID_1", new Topic("finance/+"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(financeOne);
        store.add(financeOne.asClientTopicCouple());
        assertThat(store.matches(new Topic("finance/stock"))).containsExactly(financeOne);
    }

    @Test
    public void testMatchManySingle() {
        Subscription manySub = new Subscription("FAKE_CLI_ID_1", new Topic("+/+"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(manySub);
        store.add(manySub.asClientTopicCouple());

        // verify
        assertThat(store.matches(new Topic("/finance"))).contains(manySub);
    }

    @Test
    public void testMatchSlashSingle() {
        Subscription slashPlusSub = new Subscription("FAKE_CLI_ID_1", new Topic("/+"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(slashPlusSub);
        store.add(slashPlusSub.asClientTopicCouple());
        Subscription anySub = new Subscription("FAKE_CLI_ID_1", new Topic("+"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(anySub);
        store.add(anySub.asClientTopicCouple());

        // Verify
        assertThat(store.matches(new Topic("/finance"))).containsOnly(slashPlusSub);
        assertThat(store.matches(new Topic("/finance"))).doesNotContain(anySub);
    }

    @Test
    public void testMatchManyDeepSingle() {
        Subscription slashPlusSub = new Subscription(
                "FAKE_CLI_ID_1",
                new Topic("/finance/+/ibm"),
                MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(slashPlusSub);
        store.add(slashPlusSub.asClientTopicCouple());

        Subscription slashPlusDeepSub = new Subscription(
                "FAKE_CLI_ID_2",
                new Topic("/+/stock/+"),
                MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(slashPlusDeepSub);
        store.add(slashPlusDeepSub.asClientTopicCouple());

        // Verify
        assertThat(store.matches(new Topic("/finance/stock/ibm")))
                .containsExactlyInAnyOrder(slashPlusSub, slashPlusDeepSub);
    }

    @Test
    public void testMatchSimpleMulti_allTheTree() {
        Subscription sub = new Subscription("FAKE_CLI_ID_1", new Topic("#"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(sub);
        store.add(sub.asClientTopicCouple());
        assertThat(store.matches(new Topic("finance"))).isNotEmpty();
        assertThat(store.matches(new Topic("finance/ibm"))).isNotEmpty();
    }

    @Test
    public void testMatchSimpleMulti_zeroLevel() {
        // check MULTI in case of zero level match
        Subscription sub = new Subscription("FAKE_CLI_ID_1", new Topic("finance/#"), MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(sub);
        store.add(sub.asClientTopicCouple());
        assertThat(store.matches(new Topic("finance"))).isNotEmpty();
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

    private void assertMatch(String s, String t) {
        Topic subscription = new Topic(s);
        Topic topic = new Topic(t);
        store = new SubscriptionsDirectory();
        IConfig config = new MemoryConfig(System.getProperties());
        MemoryStorageService memStore = new MemoryStorageService(config, null);
        memStore.initStore();
        ISessionsStore aSessionsStore = memStore.sessionsStore();
        aSessionsStore.createNewSession("FAKE_CLI_ID_1", false);
        store.init(aSessionsStore);
        Subscription sub = new Subscription("FAKE_CLI_ID_1", subscription, MqttQoS.AT_MOST_ONCE);
        aSessionsStore.subscriptionStore().addNewSubscription(sub);
        store.add(sub.asClientTopicCouple());
        assertThat(store.matches(topic)).isNotEmpty();
    }

    private void assertNotMatch(String s, String t) {
        Topic subscription = new Topic(s);
        Topic topic = new Topic(t);
        store = new SubscriptionsDirectory();
        IConfig config = new MemoryConfig(System.getProperties());
        MemoryStorageService memStore = new MemoryStorageService(config, null);
        memStore.initStore();
        store.init(memStore.sessionsStore());
        Subscription sub = new Subscription("FAKE_CLI_ID_1", subscription, MqttQoS.AT_MOST_ONCE);
        this.subscriptionsStore.addNewSubscription(sub);
        store.add(sub.asClientTopicCouple());
        assertThat(store.matches(topic)).isEmpty();
    }

    @Test
    public void testOverlappingSubscriptions() {
        Subscription genericSub = new Subscription("FAKE_CLI_ID_1", new Topic("a/+"), MqttQoS.EXACTLY_ONCE);
        this.subscriptionsStore.addNewSubscription(genericSub);
        store.add(genericSub.asClientTopicCouple());
        Subscription specificSub = new Subscription("FAKE_CLI_ID_1", new Topic("a/b"), MqttQoS.AT_LEAST_ONCE);
        this.subscriptionsStore.addNewSubscription(specificSub);
        store.add(specificSub.asClientTopicCouple());

        // Verify
        assertThat(store.matches(new Topic("a/b")).size()).isEqualTo(1);
    }

    @Test
    public void removeSubscription_withDifferentClients_subscribedSameTopic() {
        ISubscriptionsDirectory aStore = new SubscriptionsDirectory();
        IConfig config = new MemoryConfig(System.getProperties());
        MemoryStorageService memStore = new MemoryStorageService(config, null);
        memStore.initStore();
        ISessionsStore sessionsStore = memStore.sessionsStore();
        aStore.init(sessionsStore);
        // subscribe a not active clientID1 to /topic
        sessionsStore.createNewSession("FAKE_CLI_ID_1", true);
        Subscription slashSub = new Subscription("FAKE_CLI_ID_1", new Topic("/topic"), MqttQoS.AT_MOST_ONCE);
        sessionsStore.subscriptionStore().addNewSubscription(slashSub);
        aStore.add(slashSub.asClientTopicCouple());

        // subscribe an active clientID2 to /topic
        Subscription slashSub2 = new Subscription("FAKE_CLI_ID_2", new Topic("/topic"), MqttQoS.AT_MOST_ONCE);
        sessionsStore.subscriptionStore().addNewSubscription(slashSub2);
        aStore.add(new ClientTopicCouple("FAKE_CLI_ID_2", new Topic("/topic")));

        // Exercise
        aStore.removeSubscription(new Topic("/topic"), slashSub2.getClientId());

        // Verify
        Subscription remainedSubscription = aStore.matches(new Topic("/topic")).get(0);
        assertThat(remainedSubscription.getClientId()).isEqualTo(slashSub.getClientId());
    }

    /*
     * Test for Issue #49
     */
    @Test
    public void duplicatedSubscriptionsWithDifferentQos() {
        ClientSession session2 = sessionsStore.createNewSession("client2", true);
        Subscription client2Sub = new Subscription("client2", new Topic("client/test/b"), MqttQoS.AT_MOST_ONCE);
        session2.subscribe(client2Sub);
        store.add(client2Sub.asClientTopicCouple());
        ClientSession session1 = sessionsStore.createNewSession("client1", true);
        Subscription client1SubQoS0 = new Subscription("client1", new Topic("client/test/b"), MqttQoS.AT_MOST_ONCE);
        session1.subscribe(client1SubQoS0);
        store.add(client1SubQoS0.asClientTopicCouple());

        Subscription client1SubQoS2 = new Subscription("client1", new Topic("client/test/b"), MqttQoS.EXACTLY_ONCE);
        session1.subscribe(client1SubQoS2);
        store.add(client1SubQoS2.asClientTopicCouple());

        System.out.println(store.dumpTree());

        // Verify
        List<Subscription> subscriptions = store.matches(new Topic("client/test/b"));
        assertThat(subscriptions).contains(client1SubQoS2);
        assertThat(subscriptions).contains(client2Sub);

        Subscription client1Sub = subscriptions.get(subscriptions.indexOf(client1SubQoS0));
        assertThat(client1SubQoS0.getRequestedQos()).isNotEqualTo(client1Sub.getRequestedQos());

        // client1SubQoS2 should override client1SubQoS0
        assertThat(client1Sub.getRequestedQos()).isEqualTo(client1SubQoS2.getRequestedQos());
    }

    @Test
    public void testRecreatePath_emptyRoot() {
        TreeNode oldRoot = new TreeNode();
        final SubscriptionsDirectory.NodeCouple resp = store.recreatePath(new Topic("/finance"), oldRoot);

        // Verify
        assertThat(resp.root).isNotNull();
        assertThat(resp.root.m_token).isNull();
        assertThat(resp.root.m_children).hasSize(1);
        assertThat(resp.root.m_children.get(0).m_children).contains(resp.createdNode);
    }

    @Test
    public void testRecreatePath_1layer_tree() {
        TreeNode oldRoot = new TreeNode();
        final SubscriptionsDirectory.NodeCouple respFinance = store.recreatePath(new Topic("/finance"), oldRoot);
        final SubscriptionsDirectory.NodeCouple respPlus = store.recreatePath(new Topic("/+"), respFinance.root);

        // Verify
        assertThat(respPlus.root).isNotNull();
        assertThat(respPlus.root.m_token).isNull();
        assertThat(respPlus.root.m_children.size()).isEqualTo(1);
        assertThat(respPlus.root.m_children.get(0).m_children).contains(respPlus.createdNode);
        assertThat(respPlus.root.m_children.get(0).m_children).contains(respFinance.createdNode);
    }
}

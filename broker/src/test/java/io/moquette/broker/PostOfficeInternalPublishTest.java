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
package io.moquette.broker;

import io.moquette.broker.security.PermitAllAuthorizatorPolicy;
import io.moquette.broker.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.broker.subscriptions.ISubscriptionsDirectory;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.moquette.broker.MQTTConnectionPublishTest.memorySessionsRepository;
import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static io.moquette.broker.PostOfficeUnsubscribeTest.CONFIG;
import static io.netty.handler.codec.mqtt.MqttQoS.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.*;

public class PostOfficeInternalPublishTest {

    private static final String FAKE_CLIENT_ID = "FAKE_123";
    private static final String TEST_USER = "fakeuser";
    private static final String TEST_PWD = "fakepwd";
    private static final String PAYLOAD = "Hello MQTT World";

    private MQTTConnection connection;
    private EmbeddedChannel channel;
    private PostOffice sut;
    private ISubscriptionsDirectory subscriptions;
    private MqttConnectMessage connectMessage;
    private SessionRegistry sessionRegistry;
    private MockAuthenticator mockAuthenticator;
    private static final BrokerConfiguration ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID =
        new BrokerConfiguration(true, true, false, NO_BUFFER_FLUSH);
    private MemoryRetainedRepository retainedRepository;
    private MemoryQueueRepository queueRepository;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        initPostOfficeAndSubsystems();

        mockAuthenticator = new MockAuthenticator(singleton(FAKE_CLIENT_ID), singletonMap(TEST_USER, TEST_PWD));
        connection = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID);

        connectMessage = ConnectionTestUtils.buildConnect(FAKE_CLIENT_ID);

        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
    }

    @AfterEach
    public void tearDown() {
        scheduler.shutdown();
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config) {
        channel = new EmbeddedChannel();
        return createMQTTConnection(config, channel);
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config, Channel channel) {
        return new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, sut);
    }

    private void initPostOfficeAndSubsystems() {
        scheduler = Executors.newScheduledThreadPool(1);
        subscriptions = new CTrieSubscriptionDirectory();
        ISubscriptionsRepository subscriptionsRepository = new MemorySubscriptionsRepository();
        subscriptions.init(subscriptionsRepository);
        retainedRepository = new MemoryRetainedRepository();
        queueRepository = new MemoryQueueRepository();

        final PermitAllAuthorizatorPolicy authorizatorPolicy = new PermitAllAuthorizatorPolicy();
        final Authorizator permitAll = new Authorizator(authorizatorPolicy);
        final SessionEventLoopGroup loopsGroup = new SessionEventLoopGroup(ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, 1024);
        sessionRegistry = new SessionRegistry(subscriptions, memorySessionsRepository(), queueRepository, permitAll, scheduler, loopsGroup);
        sut = new PostOffice(subscriptions, retainedRepository, sessionRegistry,
                             ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, permitAll, loopsGroup);
    }

    private void internalPublishNotRetainedTo(String topic) {
        internalPublishTo(topic, AT_MOST_ONCE, false);
    }

    private void internalPublishRetainedTo(String topic) {
        internalPublishTo(topic, AT_MOST_ONCE, true);
    }

    private void internalPublishTo(String topic, MqttQoS qos, boolean retained) {
        MqttPublishMessage publish = MqttMessageBuilders.publish()
            .topicName(topic)
            .retained(retained)
            .qos(qos)
            .payload(Unpooled.copiedBuffer(PAYLOAD.getBytes(UTF_8))).build();
        final RoutingResults res = sut.internalPublish(publish);
        try {
            res.completableFuture().get(5, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testClientSubscribeAfterNotRetainedQoS0IsSent() {
//        connection.processConnect(connectMessage);
//        ConnectionTestUtils.assertConnectAccepted(channel);

        // Exercise
        final String topic = "/topic";
        internalPublishNotRetainedTo(topic);

        subscribe(AT_MOST_ONCE, topic, connection);
        completeEventLoopCommands();

        // Verify
        verifyNoPublishIsReceived(channel);
    }

    @Test
    public void testClientSubscribeAfterRetainedQoS0IsSent() {
        // Exercise
        final String topic = "/topic";
        internalPublishRetainedTo(topic);

        subscribe(AT_MOST_ONCE, topic, connection);
        completeEventLoopCommands();

        // Verify
        verifyNoPublishIsReceived(channel);
    }

    @Test
    public void testClientSubscribeBeforeNotRetainedQoS0IsSent() {
        subscribe(AT_MOST_ONCE, "/topic", connection);

        // Exercise
        internalPublishNotRetainedTo("/topic");
        completeEventLoopCommands();

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_MOST_ONCE, PAYLOAD);
    }

    @Test
    public void testClientSubscribeBeforeRetainedQoS0IsSent() {
        subscribe(AT_MOST_ONCE, "/topic", connection);

        // Exercise
        internalPublishRetainedTo("/topic");
        completeEventLoopCommands();

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_MOST_ONCE, PAYLOAD);
    }

    private void completeEventLoopCommands() {
        // shutdown event loop threads to complete all queued commands
        sut.terminate();
    }

    @Test
    public void testClientSubscribeBeforeNotRetainedQoS1IsSent() {
        subscribe(AT_LEAST_ONCE, "/topic", connection);

        // Exercise
        internalPublishTo("/topic", AT_LEAST_ONCE, false);
        completeEventLoopCommands();

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_LEAST_ONCE, PAYLOAD);
    }

    @Test
    public void testClientSubscribeAfterNotRetainedQoS1IsSent() {
        // Exercise
        internalPublishTo("/topic", AT_LEAST_ONCE, false);
        subscribe(AT_LEAST_ONCE, "/topic", connection);
        completeEventLoopCommands();

        // Verify
        verifyNoPublishIsReceived(channel);
    }

    @Test
    public void testClientSubscribeBeforeRetainedQoS1IsSent() throws Exception {
        subscribe(AT_LEAST_ONCE, "/topic", connection);

        // Exercise
        internalPublishTo("/topic", AT_LEAST_ONCE, true);
        completeEventLoopCommands();

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_LEAST_ONCE, PAYLOAD);
    }

    @Test
    public void testClientSubscribeAfterRetainedQoS1IsSent() {
        // Exercise
        internalPublishTo("/topic", AT_LEAST_ONCE, true);
        subscribe(AT_LEAST_ONCE, "/topic", connection);
        completeEventLoopCommands();

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_LEAST_ONCE, PAYLOAD);
    }

    @Test
    public void testClientSubscribeBeforeNotRetainedQoS2IsSent() throws Exception {
        subscribe(EXACTLY_ONCE, "/topic", connection);

        // Exercise
        internalPublishTo("/topic", EXACTLY_ONCE, false);
        completeEventLoopCommands();

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, EXACTLY_ONCE, PAYLOAD);
    }

    @Test
    public void testClientSubscribeAfterNotRetainedQoS2IsSent() {
        // Exercise
        internalPublishTo("/topic", EXACTLY_ONCE, false);
        subscribe(EXACTLY_ONCE, "/topic", connection);
        completeEventLoopCommands();

        // Verify
        verifyNoPublishIsReceived(channel);
    }

    @Test
    public void testClientSubscribeBeforeRetainedQoS2IsSent() {
        subscribe(EXACTLY_ONCE, "/topic", connection);

        // Exercise
        internalPublishTo("/topic", EXACTLY_ONCE, true);
        completeEventLoopCommands();

        // Verify
        assertNotNull(channel, "channel can't be null");
        ConnectionTestUtils.verifyPublishIsReceived(channel, EXACTLY_ONCE, PAYLOAD);
    }

    @Test
    public void testClientSubscribeAfterRetainedQoS2IsSent() {
        // Exercise
        internalPublishTo("/topic", EXACTLY_ONCE, true);
        subscribe(EXACTLY_ONCE, "/topic", connection);
        completeEventLoopCommands();

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, EXACTLY_ONCE, PAYLOAD);
    }

    @Test
    public void testClientSubscribeAfterDisconnected() {
        subscribe(AT_MOST_ONCE, "foo", connection);
        connection.processDisconnect(null);

        internalPublishTo("foo", AT_MOST_ONCE, false);
        completeEventLoopCommands();

        verifyNoPublishIsReceived(channel);
    }

    @Test
    public void testClientSubscribeWithoutCleanSession() throws ExecutionException, InterruptedException {
        subscribe(AT_MOST_ONCE, "foo", connection);
        connection.processDisconnect(null);
        assertEquals(1, subscriptions.size());

        MQTTConnection anotherConn = createMQTTConnection(CONFIG);

        MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .cleanSession(false)
            .build();
        anotherConn.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(anotherConn);

        assertEquals(1, subscriptions.size());
        internalPublishTo("foo", MqttQoS.AT_MOST_ONCE, false);
        completeEventLoopCommands();
        ConnectionTestUtils.verifyPublishIsReceived(anotherConn, AT_MOST_ONCE, PAYLOAD);
    }

    private void subscribe(MqttQoS topic, String newsTopic, MQTTConnection connection) {
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(topic, newsTopic)
            .messageId(1)
            .build();
        sut.subscribeClientToTopics(subscribe, connection.getClientId(), null, this.connection);

        MqttSubAckMessage subAck = ((EmbeddedChannel) this.connection.channel).readOutbound();
        assertEquals(topic.value(), (int) subAck.payload().grantedQoSLevels().get(0));
    }

    protected void subscribe(MQTTConnection connection, String topic, MqttQoS desiredQos) {
        EmbeddedChannel channel = (EmbeddedChannel) connection.channel;
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(desiredQos, topic)
            .messageId(1)
            .build();
        sut.subscribeClientToTopics(subscribe, connection.getClientId(), null, connection);

        MqttSubAckMessage subAck = channel.readOutbound();
        assertEquals(desiredQos.value(), (int) subAck.payload().grantedQoSLevels().get(0));

        final String clientId = connection.getClientId();
        Subscription expectedSubscription = new Subscription(clientId, new Topic(topic), desiredQos);

        final Set<Subscription> matchedSubscriptions = subscriptions.matchWithoutQosSharpening(new Topic(topic));
        assertEquals(1, matchedSubscriptions.size());
        final Subscription onlyMatchedSubscription = matchedSubscriptions.iterator().next();
        assertEquals(expectedSubscription, onlyMatchedSubscription);
    }

    private void verifyNoPublishIsReceived(EmbeddedChannel channel) {
        final Object messageReceived = channel.readOutbound();
        assertNull(messageReceived, "Received an out message from processor while not expected");
    }
}

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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;

import static io.moquette.broker.MQTTConnectionPublishTest.memorySessionsRepository;
import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static io.moquette.broker.PostOfficeUnsubscribeTest.CONFIG;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.EXACTLY_ONCE;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.*;

public class PostOfficePublishTest {

    private static final String FAKE_CLIENT_ID = "FAKE_123";
    private static final String FAKE_CLIENT_ID2 = "FAKE_456";
    static final String SUBSCRIBER_ID = "Subscriber";
    static final String PUBLISHER_ID = "Publisher";
    private static final String TEST_USER = "fakeuser";
    private static final String TEST_PWD = "fakepwd";
    private static final String NEWS_TOPIC = "/news";
    private static final String BAD_FORMATTED_TOPIC = "#MQTTClient";

    private MQTTConnection connection;
    private EmbeddedChannel channel;
    private PostOffice sut;
    private ISubscriptionsDirectory subscriptions;
    public static final String FAKE_USER_NAME = "UnAuthUser";
    private MqttConnectMessage connectMessage;
    private SessionRegistry sessionRegistry;
    private MockAuthenticator mockAuthenticator;
    static final BrokerConfiguration ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID =
        new BrokerConfiguration(true, true, false, NO_BUFFER_FLUSH);
    private MemoryRetainedRepository retainedRepository;
    private MemoryQueueRepository queueRepository;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    public void setUp() {
        initPostOfficeAndSubsystems();

        mockAuthenticator = new MockAuthenticator(singleton(FAKE_CLIENT_ID), singletonMap(TEST_USER, TEST_PWD));
        connection = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID);
        channel = (EmbeddedChannel) connection.channel;

        connectMessage = ConnectionTestUtils.buildConnect(FAKE_CLIENT_ID);
    }

    @AfterEach
    public void tearDown() {
        scheduler.shutdown();
        sut.terminate();
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config) {
        return createMQTTConnection(config, new EmbeddedChannel());
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

    @Test
    public void testPublishQoS0ToItself() throws ExecutionException, InterruptedException, TimeoutException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);

        // subscribe
        subscribe(AT_MOST_ONCE, NEWS_TOPIC, connection);

        // Exercise
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, FAKE_CLIENT_ID,
            MqttMessageBuilders.publish()
                .payload(payload.retainedDuplicate())
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build()).get(5, TimeUnit.SECONDS);

        // Verify
        ConnectionTestUtils.verifyReceivePublish(channel, NEWS_TOPIC, "Hello world!");
    }

    @Test
    public void testForceClientDisconnection_issue116() throws ExecutionException, InterruptedException, TimeoutException {
        final MQTTConnection clientXA = connectAs("subscriber");
        subscribe(clientXA, NEWS_TOPIC, AT_MOST_ONCE);

        final MQTTConnection clientXB = connectAs("publisher");
        final ByteBuf anyPayload = Unpooled.copiedBuffer("Hello", Charset.defaultCharset());
        sut.receivedPublishQos2(clientXB, MqttMessageBuilders.publish()
            .payload(anyPayload)
            .qos(MqttQoS.EXACTLY_ONCE)
            .retained(false)
            .messageId(1)
            .topicName(NEWS_TOPIC).build(), "username");

        final MQTTConnection clientYA = connectAs("subscriber");
        subscribe(clientYA, NEWS_TOPIC, AT_MOST_ONCE);

        final MQTTConnection clientYB = connectAs("publisher");
        final ByteBuf anyPayload2 = Unpooled.copiedBuffer("Hello 2", Charset.defaultCharset());
        sut.receivedPublishQos2(clientYB, MqttMessageBuilders.publish()
            .payload(anyPayload2)
            .qos(MqttQoS.EXACTLY_ONCE)
            .retained(true)
            .messageId(2)
            .topicName(NEWS_TOPIC).build(), "username").completableFuture().get(5, TimeUnit.SECONDS);

        // Verify
        assertFalse(clientXA.channel.isOpen(), "First 'subscriber' channel MUST be closed by the broker");
        ConnectionTestUtils.verifyPublishIsReceived((EmbeddedChannel) clientYA.channel, AT_MOST_ONCE, "Hello 2");
    }

    private MQTTConnection connectAs(String clientId) {
        EmbeddedChannel channel = new EmbeddedChannel();
        MQTTConnection connection = createMQTTConnection(CONFIG, channel);
        ConnectionTestUtils.connect(connection, ConnectionTestUtils.buildConnect(clientId));
        return connection;
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

    @Test
    public void testPublishToMultipleSubscribers() throws ExecutionException, InterruptedException, TimeoutException {
        final Set<String> clientIds = new HashSet<>(Arrays.asList(FAKE_CLIENT_ID, FAKE_CLIENT_ID2));
        mockAuthenticator = new MockAuthenticator(clientIds, singletonMap(TEST_USER, TEST_PWD));
        EmbeddedChannel channel1 = new EmbeddedChannel();
        MQTTConnection connection1 = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, channel1);
        connection1.processConnect(ConnectionTestUtils.buildConnect(FAKE_CLIENT_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel1);

        EmbeddedChannel channel2 = new EmbeddedChannel();
        MQTTConnection connection2 = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, channel2);
        connection2.processConnect(ConnectionTestUtils.buildConnect(FAKE_CLIENT_ID2)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel2);

        // subscribe
        final MqttQoS qos = AT_MOST_ONCE;
        final String newsTopic = NEWS_TOPIC;
        subscribe(qos, newsTopic, connection1);
        subscribe(qos, newsTopic, connection2);

        // Exercise
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, FAKE_CLIENT_ID,
            MqttMessageBuilders.publish()
                .payload(payload.retainedDuplicate())
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build()).get(5, TimeUnit.SECONDS);

        // Verify
        ConnectionTestUtils.verifyReceivePublish(channel1, NEWS_TOPIC, "Hello world!");
        ConnectionTestUtils.verifyReceivePublish(channel2, NEWS_TOPIC, "Hello world!");
    }

    @Test
    public void testPublishWithEmptyPayloadClearRetainedStore() throws ExecutionException, InterruptedException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);

        final ByteBuf payload1 = ByteBufUtil.writeAscii(UnpooledByteBufAllocator.DEFAULT, "Hello world!");
        this.retainedRepository.retain(new Topic(NEWS_TOPIC), MqttMessageBuilders.publish()
            .payload(payload1)
            .qos(AT_LEAST_ONCE)
            .build());
        // Retaining a msg does not release the payload.
        payload1.release();

        // Exercise
        final ByteBuf anyPayload = Unpooled.copiedBuffer("Any payload", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, FAKE_CLIENT_ID,
            MqttMessageBuilders.publish()
                .payload(anyPayload)
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(true)
                .topicName(NEWS_TOPIC).build());
        // receivedPublishQos0 does not release payload.
        anyPayload.release();

        // Verify
        assertTrue(retainedRepository.isEmpty(), "QoS0 MUST clean retained message for topic");
    }

    @Test
    public void testPublishWithQoS1() throws ExecutionException, InterruptedException, TimeoutException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        subscribe(connection, NEWS_TOPIC, AT_LEAST_ONCE);

        MQTTConnection senderConnection = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID);
        senderConnection.processConnect(ConnectionTestUtils.buildConnect("Publisher")).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(senderConnection);

        // Exercise
        final ByteBuf anyPayload = Unpooled.copiedBuffer("Any payload", Charset.defaultCharset());
        sut.receivedPublishQos1(senderConnection, new Topic(NEWS_TOPIC), TEST_USER, 1,
            MqttMessageBuilders.publish()
                .payload(anyPayload)
                .qos(MqttQoS.AT_LEAST_ONCE)
                .retained(true)
                .topicName(NEWS_TOPIC).build()).completableFuture().get(5, TimeUnit.SECONDS);

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_LEAST_ONCE, "Any payload");
    }

    @Test
    public void testPublishWithQoS2() throws ExecutionException, InterruptedException, TimeoutException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        subscribe(connection, NEWS_TOPIC, EXACTLY_ONCE);

        MQTTConnection senderConnection = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID);
        senderConnection.processConnect(ConnectionTestUtils.buildConnect("Publisher")).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(senderConnection);

        // Exercise
        final ByteBuf anyPayload = Unpooled.copiedBuffer("Any payload", Charset.defaultCharset());
        sut.receivedPublishQos2(senderConnection, MqttMessageBuilders.publish()
                .payload(anyPayload)
                .qos(MqttQoS.EXACTLY_ONCE)
                .retained(true)
                .messageId(2)
                .topicName(NEWS_TOPIC).build(), "username").completableFuture().get(5000, TimeUnit.SECONDS);

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, EXACTLY_ONCE, "Any payload");
    }

    // aka testPublishWithQoS1_notCleanSession
    @Test
    public void forwardQoS1PublishesWhenNotCleanSessionReconnects() throws ExecutionException, InterruptedException {
        connection.processConnect(ConnectionTestUtils.buildConnectNotClean(FAKE_CLIENT_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        subscribe(connection, NEWS_TOPIC, AT_LEAST_ONCE);
        connection.processDisconnect(null);

        // publish a QoS 1 message from another client publish a message on the topic
        EmbeddedChannel pubChannel = new EmbeddedChannel();
        MQTTConnection pubConn = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, pubChannel);
        pubConn.processConnect(ConnectionTestUtils.buildConnect(PUBLISHER_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(pubChannel);

        final ByteBuf anyPayload = Unpooled.copiedBuffer("Any payload", Charset.defaultCharset());
        sut.receivedPublishQos1(pubConn, new Topic(NEWS_TOPIC), TEST_USER, 1,
            MqttMessageBuilders.publish()
                .payload(anyPayload.retainedDuplicate())
                .qos(MqttQoS.AT_LEAST_ONCE)
                .topicName(NEWS_TOPIC).build());

        // simulate a reconnection from the other client
        connection = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID);
        channel = (EmbeddedChannel) connection.channel;
        connectMessage = ConnectionTestUtils.buildConnectNotClean(FAKE_CLIENT_ID);
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);

        // Verify
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_LEAST_ONCE, "Any payload");
    }

    @Test
    public void checkReceivePublishedMessage_after_a_reconnect_with_notCleanSession() throws ExecutionException, InterruptedException, TimeoutException {
        // first connect - subscribe -disconnect
        connection.processConnect(ConnectionTestUtils.buildConnectNotClean(FAKE_CLIENT_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        subscribe(connection, NEWS_TOPIC, AT_LEAST_ONCE);
        connection.processDisconnect(null);

        // connect - subscribe from another connection but with same ClientID
        EmbeddedChannel secondChannel = new EmbeddedChannel();
        MQTTConnection secondConn = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, secondChannel);
        secondConn.processConnect(ConnectionTestUtils.buildConnect(FAKE_CLIENT_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(secondChannel);
        subscribe(secondConn, NEWS_TOPIC, AT_LEAST_ONCE);

        // publish a QoS 1 message another client publish a message on the topic
        EmbeddedChannel pubChannel = new EmbeddedChannel();
        MQTTConnection pubConn = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, pubChannel);
        pubConn.processConnect(ConnectionTestUtils.buildConnect(PUBLISHER_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(pubChannel);

        final ByteBuf anyPayload = Unpooled.copiedBuffer("Any payload", Charset.defaultCharset());
        sut.receivedPublishQos1(pubConn, new Topic(NEWS_TOPIC), TEST_USER, 1,
            MqttMessageBuilders.publish()
                .payload(anyPayload.retainedDuplicate())
                .qos(MqttQoS.AT_LEAST_ONCE)
                .retained(true)
                .topicName(NEWS_TOPIC).build()).completableFuture().get(5, TimeUnit.SECONDS);

        // Verify that after a reconnection the client receive the message
        ConnectionTestUtils.verifyPublishIsReceived(secondChannel, AT_LEAST_ONCE, "Any payload");
    }

    @Test
    public void noPublishToInactiveSession() throws ExecutionException, InterruptedException {
        // create an inactive session for Subscriber
        connection.processConnect(ConnectionTestUtils.buildConnectNotClean(SUBSCRIBER_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        subscribe(connection, NEWS_TOPIC, AT_LEAST_ONCE);
        connection.processDisconnect(null);

        // Exercise
        EmbeddedChannel pubChannel = new EmbeddedChannel();
        MQTTConnection pubConn = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, pubChannel);
        pubConn.processConnect(ConnectionTestUtils.buildConnect(PUBLISHER_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(pubChannel);

        final ByteBuf anyPayload = Unpooled.copiedBuffer("Any payload", Charset.defaultCharset());
        sut.receivedPublishQos1(pubConn, new Topic(NEWS_TOPIC), TEST_USER, 1,
            MqttMessageBuilders.publish()
                .payload(anyPayload)
                .qos(MqttQoS.AT_LEAST_ONCE)
                .retained(true)
                .topicName(NEWS_TOPIC).build());

        verifyNoPublishIsReceived(channel);
    }

    private void verifyNoPublishIsReceived(EmbeddedChannel channel) {
        final Object messageReceived = channel.readOutbound();
        assertNull(messageReceived, "Received an out message from processor while not expected");
    }

    @Test
    public void cleanRetainedMessageStoreWhenPublishWithRetainedQos0IsReceived() throws ExecutionException, InterruptedException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);

        MQTTConnection senderConnection = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID);
        senderConnection.processConnect(ConnectionTestUtils.buildConnect("Publisher")).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(senderConnection);

        // publish a QoS1 retained message
        final ByteBuf anyPayload = Unpooled.copiedBuffer("Any payload", Charset.defaultCharset());
        final MqttPublishMessage publishMsg = MqttMessageBuilders.publish()
            .payload(Unpooled.copiedBuffer("Any payload", Charset.defaultCharset()))
            .qos(MqttQoS.AT_LEAST_ONCE)
            .retained(true)
            .topicName(NEWS_TOPIC)
            .build();
        sut.receivedPublishQos1(senderConnection, new Topic(NEWS_TOPIC), TEST_USER, 1,
                                publishMsg);

        assertMessageIsRetained(NEWS_TOPIC, anyPayload);

        // publish a QoS0 retained message
        // Exercise
        final ByteBuf qos0Payload = Unpooled.copiedBuffer("QoS0 payload", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, connection.getClientId(),
            MqttMessageBuilders.publish()
                .payload(qos0Payload)
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(true)
                .topicName(NEWS_TOPIC).build());

        // Verify
        assertTrue(retainedRepository.isEmpty(), "Retained message for topic /news must be cleared");
    }

    private void assertMessageIsRetained(String expectedTopicName, ByteBuf expectedPayload) {
        List<RetainedMessage> msgs = retainedRepository.retainedOnTopic(expectedTopicName);
        assertEquals(1, msgs.size());
        RetainedMessage msg = msgs.get(0);
        assertEquals(ByteBufUtil.hexDump(expectedPayload), ByteBufUtil.hexDump(msg.getPayload()));
    }
}

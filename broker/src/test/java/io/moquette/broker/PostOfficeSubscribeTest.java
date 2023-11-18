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
import io.moquette.broker.security.IAuthenticator;
import io.moquette.broker.security.IAuthorizatorPolicy;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static io.moquette.broker.MQTTConnectionPublishTest.memorySessionsRepository;
import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static io.moquette.broker.PostOfficePublishTest.ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID;
import static io.moquette.broker.PostOfficePublishTest.SUBSCRIBER_ID;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.EXACTLY_ONCE;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostOfficeSubscribeTest {

    private static final String FAKE_CLIENT_ID = "FAKE_123";
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
    private IAuthenticator mockAuthenticator;
    private SessionRegistry sessionRegistry;
    public static final BrokerConfiguration CONFIG = new BrokerConfiguration(true, true, false, NO_BUFFER_FLUSH);
    private MemoryQueueRepository queueRepository;
    private ScheduledExecutorService scheduler;
    private ISessionsRepository fakeSessionRepo;

    @BeforeEach
    public void setUp() {
        connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .build();

        prepareSUT();
        createMQTTConnection(CONFIG);
    }

    @AfterEach
    public void tearDown() {
        scheduler.shutdown();
    }

    private void createMQTTConnection(BrokerConfiguration config) {
        channel = new EmbeddedChannel();
        connection = createMQTTConnection(config, channel);
    }

    private void prepareSUT() {
        scheduler = Executors.newScheduledThreadPool(1);
        mockAuthenticator = new MockAuthenticator(singleton(FAKE_CLIENT_ID), singletonMap(TEST_USER, TEST_PWD));

        subscriptions = new CTrieSubscriptionDirectory();
        ISubscriptionsRepository subscriptionsRepository = new MemorySubscriptionsRepository();
        subscriptions.init(subscriptionsRepository);
        queueRepository = new MemoryQueueRepository();

        final PermitAllAuthorizatorPolicy authorizatorPolicy = new PermitAllAuthorizatorPolicy();
        final Authorizator permitAll = new Authorizator(authorizatorPolicy);
        final SessionEventLoopGroup loopsGroup = new SessionEventLoopGroup(ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, 1024);
        fakeSessionRepo = memorySessionsRepository();
        sessionRegistry = new SessionRegistry(subscriptions, fakeSessionRepo, queueRepository, permitAll, scheduler, loopsGroup);
        sut = new PostOffice(subscriptions, new MemoryRetainedRepository(), sessionRegistry, fakeSessionRepo,
                             ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, permitAll, loopsGroup);
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config, Channel channel) {
        return new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, sut);
    }

    protected void connect() {
        MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .build();
        connection.processConnect(connectMessage);
        MqttConnAckMessage connAck = channel.readOutbound();
        assertEquals(CONNECTION_ACCEPTED, connAck.variableHeader().connectReturnCode(), "Connect must be accepted");
    }

    @Test
    public void testSubscribe() throws ExecutionException, InterruptedException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);

        // Exercise & verify
        subscribe(channel, NEWS_TOPIC, AT_MOST_ONCE);
    }

    protected void subscribe(EmbeddedChannel channel, String topic, MqttQoS desiredQos) {
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(desiredQos, topic)
            .messageId(1)
            .build();
        sut.subscribeClientToTopics(subscribe, FAKE_CLIENT_ID, null, connection);

        MqttSubAckMessage subAck = channel.readOutbound();
        assertEquals(desiredQos.value(), (int) subAck.payload().grantedQoSLevels().get(0));

        final String clientId = NettyUtils.clientID(channel);
        Subscription expectedSubscription = new Subscription(clientId, new Topic(topic), desiredQos);

        final List<Subscription> matchedSubscriptions = subscriptions.matchWithoutQosSharpening(new Topic(topic));
        assertEquals(1, matchedSubscriptions.size());
        final Subscription onlyMatchedSubscription = matchedSubscriptions.iterator().next();
        assertEquals(expectedSubscription, onlyMatchedSubscription);
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

        final List<Subscription> matchedSubscriptions = subscriptions.matchWithoutQosSharpening(new Topic(topic));
        assertEquals(1, matchedSubscriptions.size());
        final Subscription onlyMatchedSubscription = matchedSubscriptions.iterator().next();
        assertEquals(expectedSubscription, onlyMatchedSubscription);
    }

    @Test
    public void testSubscribedToNotAuthorizedTopic() throws ExecutionException, InterruptedException {
        NettyUtils.userName(channel, FAKE_USER_NAME);

        IAuthorizatorPolicy prohibitReadOnNewsTopic = mock(IAuthorizatorPolicy.class);
        when(prohibitReadOnNewsTopic.canRead(eq(new Topic(NEWS_TOPIC)), eq(FAKE_USER_NAME), eq(FAKE_CLIENT_ID)))
            .thenReturn(false);

        final SessionEventLoopGroup loopsGroup = new SessionEventLoopGroup(ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, 1024);
        sut = new PostOffice(subscriptions, new MemoryRetainedRepository(), sessionRegistry, fakeSessionRepo,
                             ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, new Authorizator(prohibitReadOnNewsTopic), loopsGroup);

        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);

        //Exercise
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(AT_MOST_ONCE, NEWS_TOPIC)
            .messageId(1)
            .build();
        sut.subscribeClientToTopics(subscribe, FAKE_CLIENT_ID, FAKE_USER_NAME, connection);

        // Verify
        MqttSubAckMessage subAckMsg = channel.readOutbound();
        verifyFailureQos(subAckMsg);
    }

    private void verifyFailureQos(MqttSubAckMessage subAckMsg) {
        List<Integer> grantedQoSes = subAckMsg.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size());
        assertTrue(grantedQoSes.contains(MqttQoS.FAILURE.value()));
    }

    @Test
    public void testDoubleSubscribe() throws ExecutionException, InterruptedException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        assertEquals(0, subscriptions.size(), "After CONNECT subscription MUST be empty");
        subscribe(channel, NEWS_TOPIC, AT_MOST_ONCE);
        assertEquals(1, subscriptions.size(), "After /news subscribe, subscription MUST contain it");

        //Exercise & verify
        subscribe(channel, NEWS_TOPIC, AT_MOST_ONCE);
    }

    @Test
    public void testSubscribeWithBadFormattedTopic() throws ExecutionException, InterruptedException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        assertEquals(0, subscriptions.size(), "After CONNECT subscription MUST be empty");

        //Exercise
        MqttSubscribeMessage subscribe = MqttMessageBuilders.subscribe()
            .addSubscription(AT_MOST_ONCE, BAD_FORMATTED_TOPIC)
            .messageId(1)
            .build();
        this.sut.subscribeClientToTopics(subscribe, FAKE_CLIENT_ID, FAKE_USER_NAME, connection);
        MqttSubAckMessage subAckMsg = channel.readOutbound();

        assertEquals(0, subscriptions.size(), "Bad topic CAN'T add any subscription");
        verifyFailureQos(subAckMsg);
    }

    @Test
    public void testCleanSession_maintainClientSubscriptions() throws ExecutionException, InterruptedException, TimeoutException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        assertEquals(0, subscriptions.size(), "After CONNECT subscription MUST be empty");

        subscribe(channel, NEWS_TOPIC, AT_MOST_ONCE);

        assertEquals(1, subscriptions.size(), "Subscribe MUST contain one subscription");

        connection.processDisconnect(null);
        assertEquals(1, subscriptions.size(), "Disconnection MUSTN'T clear subscriptions");

        EmbeddedChannel anotherChannel = new EmbeddedChannel();
        MQTTConnection anotherConn = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, anotherChannel);
        anotherConn.processConnect(ConnectionTestUtils.buildConnect(FAKE_CLIENT_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(anotherChannel);
        assertEquals(1, subscriptions.size(), "After a reconnect, subscription MUST be still present");

        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD,
            MqttMessageBuilders.publish()
                .payload(payload.retainedDuplicate())
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build()).get(5, TimeUnit.SECONDS);

        ConnectionTestUtils.verifyPublishIsReceived(anotherChannel, AT_MOST_ONCE, "Hello world!");
    }

    /**
     * Check that after a client has connected with clean session false, subscribed to some topic
     * and exited, if it reconnects with clean session true, the broker correctly cleanup every
     * previous subscription
     */
    @Test
    public void testCleanSession_correctlyClientSubscriptions() throws ExecutionException, InterruptedException, TimeoutException {
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        assertEquals(0, subscriptions.size(), "After CONNECT subscription MUST be empty");

        //subscribe(channel, NEWS_TOPIC, AT_MOST_ONCE);
        final MqttSubscribeMessage subscribeMsg = MqttMessageBuilders
            .subscribe()
            .addSubscription(AT_MOST_ONCE, NEWS_TOPIC)
            .messageId(1)
            .build();
        connection.processSubscribe(subscribeMsg).completableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(1, subscriptions.size(), "Subscribe MUST contain one subscription");

        connection.processDisconnect(null).completableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(1, subscriptions.size(), "Disconnection MUSTN'T clear subscriptions");

        connectMessage = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .cleanSession(true)
            .build();
        channel = new EmbeddedChannel();
        connection = createMQTTConnection(CONFIG, channel);
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        assertEquals(0, subscriptions.size(), "After CONNECT with clean, subscription MUST be empty");

        // publish on /news
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD,
            MqttMessageBuilders.publish()
                .payload(payload)
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build());

        // verify no publish is fired
        ConnectionTestUtils.verifyNoPublishIsReceived(channel);
    }

    @Test
    public void testReceiveRetainedPublishRespectingSubscriptionQoSAndNotPublisher() throws ExecutionException, InterruptedException {
        // publisher publish a retained message on topic /news
        connection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(channel);
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        final MqttPublishMessage retainedPubQoS1Msg = MqttMessageBuilders.publish()
            .payload(payload.retainedDuplicate())
            .qos(MqttQoS.AT_LEAST_ONCE)
            .retained(true)
            .topicName(NEWS_TOPIC).build();
        sut.receivedPublishQos1(connection, new Topic(NEWS_TOPIC), TEST_USER, 1,
            retainedPubQoS1Msg);

        // subscriber connects subscribe to topic /news and receive the last retained message
        EmbeddedChannel subChannel = new EmbeddedChannel();
        MQTTConnection subConn = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZERO_BYTES_CLID, subChannel);
        subConn.processConnect(ConnectionTestUtils.buildConnect(SUBSCRIBER_ID)).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(subChannel);
        subscribe(subConn, NEWS_TOPIC, MqttQoS.AT_MOST_ONCE);

        // Verify publish is received
        ConnectionTestUtils.verifyReceiveRetainedPublish(subChannel, NEWS_TOPIC, "Hello world!", MqttQoS.AT_MOST_ONCE);
    }

    @Test
    public void testLowerTheQosToTheRequestedBySubscription() {
        Subscription subQos1 = new Subscription("Sub A", new Topic("a/b"), MqttQoS.AT_LEAST_ONCE);
        assertEquals(MqttQoS.AT_LEAST_ONCE, PostOffice.lowerQosToTheSubscriptionDesired(subQos1, EXACTLY_ONCE));

        Subscription subQos2 = new Subscription("Sub B", new Topic("a/+"), EXACTLY_ONCE);
        assertEquals(EXACTLY_ONCE, PostOffice.lowerQosToTheSubscriptionDesired(subQos2, EXACTLY_ONCE));
    }

    @Test
    public void testExtractShareName() {
        assertEquals("", PostOffice.extractShareName("$share//measures/+/1"));
        assertEquals("myShared", PostOffice.extractShareName("$share/myShared/measures/+/1"));
        assertEquals("#", PostOffice.extractShareName("$share/#/measures/+/1"));
    }
}

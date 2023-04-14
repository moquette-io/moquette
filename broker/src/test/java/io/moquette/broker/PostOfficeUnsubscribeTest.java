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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;

import static io.moquette.broker.MQTTConnectionPublishTest.memorySessionsRepository;
import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static io.moquette.broker.PostOfficePublishTest.PUBLISHER_ID;
import static io.netty.handler.codec.mqtt.MqttQoS.*;
import static java.util.Collections.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class PostOfficeUnsubscribeTest {

    private static final String FAKE_CLIENT_ID = "FAKE_123";
    private static final String TEST_USER = "fakeuser";
    private static final String TEST_PWD = "fakepwd";
    static final String NEWS_TOPIC = "/news";
    private static final String BAD_FORMATTED_TOPIC = "#MQTTClient";

    private MQTTConnection connection;
    private EmbeddedChannel channel;
    private PostOffice sut;
    private ISubscriptionsDirectory subscriptions;
    private MqttConnectMessage connectMessage;
    private IAuthenticator mockAuthenticator;
    private SessionRegistry sessionRegistry;
    public static final BrokerConfiguration CONFIG = new BrokerConfiguration(true, true, false, NO_BUFFER_FLUSH);
    private MemoryQueueRepository queueRepository;
    private ScheduledExecutorService scheduler;

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
        sessionRegistry = new SessionRegistry(subscriptions, memorySessionsRepository(), queueRepository, permitAll, scheduler, loopsGroup);
        sut = new PostOffice(subscriptions, new MemoryRetainedRepository(), sessionRegistry,
                             ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, permitAll, loopsGroup);
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config, Channel channel) {
        return new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, sut);
    }

    protected static void connect(MQTTConnection connection, String clientId) {
        MqttConnectMessage connectMessage = ConnectionTestUtils.buildConnect(clientId);
        ConnectionTestUtils.connect(connection, connectMessage);
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

        final Set<Subscription> matchedSubscriptions = subscriptions.matchQosSharpening(new Topic(topic));
        assertEquals(1, matchedSubscriptions.size());
        //assertTrue(matchedSubscriptions.size() >=1);
        final Subscription onlyMatchedSubscription = matchedSubscriptions.iterator().next();
        assertEquals(expectedSubscription, onlyMatchedSubscription);

//        assertTrue(matchedSubscriptions.contains(expectedSubscription));
    }

    @Test
    public void testUnsubscribeWithBadFormattedTopic() {
        connect(this.connection, FAKE_CLIENT_ID);

        // Exercise
        sut.unsubscribe(singletonList(BAD_FORMATTED_TOPIC), connection, 1);

        // Verify
        assertFalse(channel.isOpen(), "Unsubscribe with bad topic MUST close drop the connection, (issue 68)");
    }

    @Test
    public void testDontNotifyClientSubscribedToTopicAfterDisconnectedAndReconnectOnSameChannel() throws ExecutionException, InterruptedException, TimeoutException {
        connect(this.connection, FAKE_CLIENT_ID);
        subscribe(connection, NEWS_TOPIC, AT_MOST_ONCE);

        // publish on /news
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD,
            MqttMessageBuilders.publish()
                .payload(payload.retainedDuplicate())
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build()).get(5, TimeUnit.SECONDS);

        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_MOST_ONCE, "Hello world!");

        unsubscribeAndVerifyAck(NEWS_TOPIC);

        // publish on /news
        final ByteBuf payload2 = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD,
            MqttMessageBuilders.publish()
                .payload(payload2)
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build());

        ConnectionTestUtils.verifyNoPublishIsReceived(channel);
    }

    protected void unsubscribeAndVerifyAck(String topic) {
        final int messageId = 1;

        sut.unsubscribe(Collections.singletonList(topic), connection, messageId);

        MqttUnsubAckMessage unsubAckMessageAck = channel.readOutbound();
        assertEquals(messageId, unsubAckMessageAck.variableHeader().messageId(), "Unsubscribe must be accepted");
    }

    @Test
    public void testDontNotifyClientSubscribedToTopicAfterDisconnectedAndReconnectOnNewChannel() throws ExecutionException, InterruptedException, TimeoutException {
        connect(this.connection, FAKE_CLIENT_ID);
        subscribe(connection, NEWS_TOPIC, AT_MOST_ONCE);
        // publish on /news
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD,
            MqttMessageBuilders.publish()
                .payload(payload.retainedDuplicate())
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build()).get(5, TimeUnit.SECONDS);

        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_MOST_ONCE, "Hello world!");

        unsubscribeAndVerifyAck(NEWS_TOPIC);
        connection.processDisconnect(null);

        // connect on another channel
        EmbeddedChannel anotherChannel = new EmbeddedChannel();
        MQTTConnection anotherConnection = createMQTTConnection(CONFIG, anotherChannel);
        anotherConnection.processConnect(connectMessage).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(anotherChannel);

        // publish on /news
        final ByteBuf payload2 = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD,
            MqttMessageBuilders.publish()
                .payload(payload2)
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build());

        ConnectionTestUtils.verifyNoPublishIsReceived(anotherChannel);
    }

    @Test
    public void avoidMultipleNotificationsAfterMultipleReconnection_cleanSessionFalseQoS1() throws ExecutionException, InterruptedException {
        final MqttConnectMessage notCleanConnect = ConnectionTestUtils.buildConnectNotClean(FAKE_CLIENT_ID);
        ConnectionTestUtils.connect(connection, notCleanConnect);
        subscribe(connection, NEWS_TOPIC, AT_LEAST_ONCE);
        connection.processDisconnect(null);

        // connect on another channel
        final String firstPayload = "Hello MQTT 1";
        connectPublishDisconnectFromAnotherClient(firstPayload, NEWS_TOPIC);

        // reconnect FAKE_CLIENT on another channel
        EmbeddedChannel anotherChannel2 = new EmbeddedChannel();
        MQTTConnection anotherConnection2 = createMQTTConnection(CONFIG, anotherChannel2);
        anotherConnection2.processConnect(notCleanConnect).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(anotherChannel2);

        ConnectionTestUtils.verifyPublishIsReceived(anotherChannel2, MqttQoS.AT_LEAST_ONCE, firstPayload);

        anotherConnection2.processDisconnect(null);

        final String secondPayload = "Hello MQTT 2";
        connectPublishDisconnectFromAnotherClient(secondPayload, NEWS_TOPIC);

        EmbeddedChannel anotherChannel3 = new EmbeddedChannel();
        MQTTConnection anotherConnection3 = createMQTTConnection(CONFIG, anotherChannel3);
        anotherConnection3.processConnect(notCleanConnect).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(anotherChannel3);

        ConnectionTestUtils.verifyPublishIsReceived(anotherChannel3, MqttQoS.AT_LEAST_ONCE, secondPayload);
    }

    private void connectPublishDisconnectFromAnotherClient(String firstPayload, String topic) {
        MQTTConnection anotherConnection = connectNotCleanAs(PUBLISHER_ID);

        // publish from another channel
        final ByteBuf anyPayload = Unpooled.copiedBuffer(firstPayload, Charset.defaultCharset());
        sut.receivedPublishQos1(anotherConnection, new Topic(topic), TEST_USER, 1,
            MqttMessageBuilders.publish()
                .payload(anyPayload)
                .qos(MqttQoS.AT_LEAST_ONCE)
                .retained(false)
                .topicName(topic).build());

        // disconnect the other channel
        anotherConnection.processDisconnect(null);
    }

    private MQTTConnection connectNotCleanAs(String clientId) {
        EmbeddedChannel channel = new EmbeddedChannel();
        MQTTConnection connection = createMQTTConnection(CONFIG, channel);
        ConnectionTestUtils.connect(connection, ConnectionTestUtils.buildConnectNotClean(clientId));
        return connection;
    }

    private MQTTConnection connectAs(String clientId) {
        EmbeddedChannel channel = new EmbeddedChannel();
        MQTTConnection connection = createMQTTConnection(CONFIG, channel);
        ConnectionTestUtils.connect(connection, ConnectionTestUtils.buildConnect(clientId));
        return connection;
    }

    @Test
    public void testConnectSubPub_cycle_getTimeout_on_second_disconnect_issue142() throws ExecutionException, InterruptedException, TimeoutException {
        connect(connection, FAKE_CLIENT_ID);
        subscribe(connection, NEWS_TOPIC, AT_MOST_ONCE);
        // publish on /news
        final ByteBuf payload = Unpooled.copiedBuffer("Hello world!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD,
            MqttMessageBuilders.publish()
                .payload(payload.retainedDuplicate())
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build()).get(5, TimeUnit.SECONDS);

        ConnectionTestUtils.verifyPublishIsReceived((EmbeddedChannel) connection.channel, AT_MOST_ONCE, "Hello world!");

        connection.processDisconnect(null);

        final MqttConnectMessage notCleanConnect = ConnectionTestUtils.buildConnect(FAKE_CLIENT_ID);
        EmbeddedChannel subscriberChannel = new EmbeddedChannel();
        MQTTConnection subscriberConnection = createMQTTConnection(CONFIG, subscriberChannel);
        subscriberConnection.processConnect(notCleanConnect).completableFuture().get();
        ConnectionTestUtils.assertConnectAccepted(subscriberChannel);

        subscribe(subscriberConnection, NEWS_TOPIC, AT_MOST_ONCE);
        // publish on /news
        final ByteBuf payload2 = Unpooled.copiedBuffer("Hello world2!", Charset.defaultCharset());
        sut.receivedPublishQos0(new Topic(NEWS_TOPIC), TEST_USER, TEST_PWD,
            MqttMessageBuilders.publish()
                .payload(payload2.retainedDuplicate())
                .qos(MqttQoS.AT_MOST_ONCE)
                .retained(false)
                .topicName(NEWS_TOPIC).build()).get(5, TimeUnit.SECONDS);

        ConnectionTestUtils.verifyPublishIsReceived(subscriberChannel, AT_MOST_ONCE, "Hello world2!");

        subscriberConnection.processDisconnect(null).completableFuture().get();

        assertFalse(subscriberChannel.isOpen(), "after a disconnect the client should be disconnected");
    }

    @Test
    public void checkReplayofStoredPublishResumeAfter_a_disconnect_cleanSessionFalseQoS1() throws ExecutionException, InterruptedException {
        final MQTTConnection publisher = connectAs("Publisher");

        connect(this.connection, FAKE_CLIENT_ID);
        subscribe(connection, NEWS_TOPIC, AT_LEAST_ONCE);

        // publish from another channel
        publishQos1(publisher, NEWS_TOPIC, "Hello world MQTT!!-1", 99);
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_LEAST_ONCE, "Hello world MQTT!!-1");
        connection.processDisconnect(null).completableFuture().get();

        publishQos1(publisher, NEWS_TOPIC, "Hello world MQTT!!-2", 100);
        publishQos1(publisher, NEWS_TOPIC, "Hello world MQTT!!-3", 101);

        createMQTTConnection(CONFIG);
        connect(this.connection, FAKE_CLIENT_ID);
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_LEAST_ONCE, "Hello world MQTT!!-2");
        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_LEAST_ONCE, "Hello world MQTT!!-3");
    }

    private void publishQos1(MQTTConnection publisher, String topic, String payload, int messageID) {
        final ByteBuf bytePayload = Unpooled.copiedBuffer(payload, Charset.defaultCharset());
        try {
            sut.receivedPublishQos1(publisher, new Topic(topic), TEST_USER, messageID,
                MqttMessageBuilders.publish()
                    .payload(bytePayload)
                    .qos(MqttQoS.AT_LEAST_ONCE)
                    .retained(false)
                    .topicName(NEWS_TOPIC).build()).completableFuture().get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void publishQos2(MQTTConnection connection, String topic, String payload) {
        final ByteBuf bytePayload = Unpooled.copiedBuffer(payload, Charset.defaultCharset());
        sut.receivedPublishQos2(connection, MqttMessageBuilders.publish()
            .payload(bytePayload)
            .qos(MqttQoS.EXACTLY_ONCE)
            .retained(true)
            .messageId(1)
            .topicName(topic).build(), "username");
    }

    /**
     * subscriber connect and subscribe on "topic" subscriber disconnects publisher connects and
     * send two message "hello1" "hello2" to "topic" subscriber connects again and receive "hello1"
     * "hello2"
     */
    @Test
    public void checkQoS2SubscriberDisconnectReceivePersistedPublishes() {
        connect(connection, FAKE_CLIENT_ID);
        subscribe(connection, NEWS_TOPIC, EXACTLY_ONCE);
        connection.processDisconnect(null);

        final MQTTConnection publisher = connectAs("Publisher");
        publishQos2(publisher, NEWS_TOPIC, "Hello world MQTT!!-1");
        publishQos2(publisher, NEWS_TOPIC, "Hello world MQTT!!-2");

        createMQTTConnection(CONFIG);
        connect(this.connection, FAKE_CLIENT_ID);
        ConnectionTestUtils.verifyPublishIsReceived(channel, EXACTLY_ONCE, "Hello world MQTT!!-1");
        ConnectionTestUtils.verifyPublishIsReceived(channel, EXACTLY_ONCE, "Hello world MQTT!!-2");
    }

    /**
     * subscriber connect and subscribe on "a/b" QoS 1 and "a/+" QoS 2 publisher connects and send a
     * message "hello" on "a/b" subscriber must receive only a single message not twice
     */
    @Test
    public void checkSinglePubPostOfficeUnsubscribeTestlishOnOverlappingSubscriptions() {
        final MQTTConnection publisher = connectAs("Publisher");

        connect(this.connection, FAKE_CLIENT_ID);
        subscribe(connection, "a/b", AT_LEAST_ONCE);
        subscribe(connection, "a/+", EXACTLY_ONCE);

        // force the publisher to send
        publishQos1(publisher, "a/b", "Hello world MQTT!!", 60);

        ConnectionTestUtils.verifyPublishIsReceived(channel, AT_LEAST_ONCE, "Hello world MQTT!!");
        ConnectionTestUtils.verifyNoPublishIsReceived(channel);
    }
}

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

import io.moquette.BrokerConstants;
import io.moquette.broker.security.PermitAllAuthorizatorPolicy;
import io.moquette.broker.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.broker.subscriptions.ISubscriptionsDirectory;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.persistence.MemorySessionsRepository;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.moquette.BrokerConstants.DISABLED_TOPIC_ALIAS;
import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MQTTConnectionPublishTest {

    public static final int DEFAULT_TOPIC_ALIAS_MAXIMUM = 16;
    private static final String FAKE_CLIENT_ID = "FAKE_123";
    private static final String TEST_USER = "fakeuser";
    private static final String TEST_PWD = "fakepwd";

    private MQTTConnection sut;
    private EmbeddedChannel channel;
    private SessionRegistry sessionRegistry;
    private MemoryQueueRepository queueRepository;
    private ScheduledExecutorService scheduler;
    private ByteBuf payload;
    private BlockingQueue<MqttPublishMessage> forwardedPublishes;

    @BeforeEach
    public void setUp() {
        forwardedPublishes = new LinkedBlockingQueue<>();

        BrokerConfiguration config = new BrokerConfiguration(true, false, true,
            false, NO_BUFFER_FLUSH, BrokerConstants.INFLIGHT_WINDOW_SIZE, DEFAULT_TOPIC_ALIAS_MAXIMUM);

        scheduler = Executors.newScheduledThreadPool(1);

        createMQTTConnection(config);

        payload = Unpooled.copiedBuffer("Hello MQTT world!".getBytes(UTF_8));
    }

    @AfterEach
    public void tearDown() {
        payload.release();
        scheduler.shutdown();
    }

    private void createMQTTConnection(BrokerConfiguration config) {
        channel = new EmbeddedChannel();
        NettyUtils.clientID(channel, "test_client");
        sut = createMQTTConnection(config, channel);
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config, Channel channel) {
        IAuthenticator mockAuthenticator = new MockAuthenticator(singleton(FAKE_CLIENT_ID),
                                                                 singletonMap(TEST_USER, TEST_PWD));

        ISubscriptionsDirectory subscriptions = new CTrieSubscriptionDirectory();
        ISubscriptionsRepository subscriptionsRepository = new MemorySubscriptionsRepository();
        subscriptions.init(subscriptionsRepository);
        queueRepository = new MemoryQueueRepository();

        final PermitAllAuthorizatorPolicy authorizatorPolicy = new PermitAllAuthorizatorPolicy();
        final Authorizator permitAll = new Authorizator(authorizatorPolicy);
        final SessionEventLoopGroup loopsGroup = new SessionEventLoopGroup(ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, 1024);
        ISessionsRepository fakeSessionRepo = memorySessionsRepository();
        sessionRegistry = new SessionRegistry(subscriptions, fakeSessionRepo, queueRepository, permitAll, scheduler, loopsGroup);
        final PostOffice postOffice = new PostOffice(subscriptions,
            new MemoryRetainedRepository(), sessionRegistry, fakeSessionRepo, ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, permitAll, loopsGroup) {

            // mock the publish forwarder method
            @Override
            CompletableFuture<Void> receivedPublishQos0(MQTTConnection connection, String username, String clientID,
                                                        MqttPublishMessage msg,
                                                        Instant messageExpiry) {
                forwardedPublishes.add(msg);
                return null;
            }

            @Override
            RoutingResults receivedPublishQos1(MQTTConnection connection, String username, int messageID,
                                               MqttPublishMessage msg, Instant messageExpiry) {
                forwardedPublishes.add(msg);
                return null;
            }

            @Override
            RoutingResults receivedPublishQos2(MQTTConnection connection, MqttPublishMessage msg, String username,
                                               Instant messageExpiry) {
                forwardedPublishes.add(msg);
                return null;
            }
        };
        return new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, postOffice);
    }

    static ISessionsRepository memorySessionsRepository() {
        return new MemorySessionsRepository();
    }

    @Test
    public void dropConnectionOnPublishWithInvalidTopicFormat() throws ExecutionException, InterruptedException {
        // Connect message with clean session set to true and client id is null.
        MqttPublishMessage publish = MqttMessageBuilders.publish()
            .topicName("")
            .retained(false)
            .qos(MqttQoS.AT_MOST_ONCE)
            .payload(payload).build();

        sut.processPublish(publish).completableFuture().get();

        // Verify
        assertFalse(channel.isOpen(), "Connection should be closed by the broker");
    }

    @Test
    public void givenPublishMessageWithInvalidTopicAliasThenConnectionDisconnects() {
        connectMqtt5AndVerifyAck(sut);

        MqttPublishMessage publish = createPublishWithTopicNameAndTopicAlias("kitchen/blinds", 0);

        // Exercise
        PostOffice.RouteResult pubResult = sut.processPublish(publish);

        // Verify
        verifyConnectionIsDropped(pubResult, MqttReasonCodes.Disconnect.TOPIC_ALIAS_INVALID);
    }

    @Test
    public void givenPublishMessageWithUnmappedTopicAliasThenPublishMessageIsForwardedWithoutAliasAndJustTopicName() throws InterruptedException {
        connectMqtt5AndVerifyAck(sut);

        String topicName = "kitchen/blinds";
        MqttPublishMessage publish = createPublishWithTopicNameAndTopicAlias(topicName, 10);

        // Exercise
        PostOffice.RouteResult pubResult = sut.processPublish(publish);

        // Verify
        assertNotNull(pubResult);
        assertTrue(pubResult.isSuccess());
        assertTrue(channel.isOpen(), "Connection should be open");

        // Read the forwarded publish message
        MqttPublishMessage reshapedPublish = forwardedPublishes.poll(1, TimeUnit.SECONDS);
        assertNotNull(reshapedPublish, "Wait time expired on reading forwarded publish message");
        assertEquals(topicName, reshapedPublish.variableHeader().topicName());
        verifyNotContainsProperty(reshapedPublish.variableHeader(), MqttProperties.MqttPropertyType.TOPIC_ALIAS);
    }

    @Test
    public void givenPublishMessageWithAlreadyMappedTopicAliasThenPublishMessageIsForwardedWithoutAliasAndJustTopicName() throws InterruptedException {
        connectMqtt5AndVerifyAck(sut);

        String topicName = "kitchen/blinds";
        MqttPublishMessage publish = createPublishWithTopicNameAndTopicAlias(topicName, 10);

        // setup the alias mapping with a first publish message
        PostOffice.RouteResult pubResult = sut.processPublish(publish);
        assertNotNull(pubResult);
        assertTrue(pubResult.isSuccess());
        assertTrue(channel.isOpen(), "Connection should be open");
        MqttPublishMessage reshapedPublish = forwardedPublishes.poll(1, TimeUnit.SECONDS);
        assertNotNull(reshapedPublish, "Wait time expired on reading forwarded publish message");

        // Exercise, use the mapped alias
        MqttPublishMessage publishWithJustAlias = createPublishWithTopicNameAndTopicAlias(10);
        pubResult = sut.processPublish(publishWithJustAlias);

        // Verify
        assertNotNull(pubResult);
        assertTrue(pubResult.isSuccess());
        assertTrue(channel.isOpen(), "Connection should be open");

        // Read the forwarded publish message
        reshapedPublish = forwardedPublishes.poll(1, TimeUnit.SECONDS);
        assertNotNull(reshapedPublish, "Wait time expired on reading forwarded publish message");
        assertEquals(topicName, reshapedPublish.variableHeader().topicName());
        verifyNotContainsProperty(reshapedPublish.variableHeader(), MqttProperties.MqttPropertyType.TOPIC_ALIAS);
    }

    @Test
    public void givenPublishMessageWithUnmappedTopicAliasAndEmptyTopicNameThenConnectionDisconnects() {
        connectMqtt5AndVerifyAck(sut);

        MqttPublishMessage publish = createPublishWithTopicNameAndTopicAlias("", 10);

        // Exercise
        PostOffice.RouteResult pubResult = sut.processPublish(publish);

        // Verify
        verifyConnectionIsDropped(pubResult, MqttReasonCodes.Disconnect.PROTOCOL_ERROR);
    }

    @Test
    public void givenTopicAliasDisabledWhenPublishContainingTopicAliasIsReceivedThenConnectionIsDropped() {
        // create a configuration with topic alias disabled
        BrokerConfiguration config = new BrokerConfiguration(true, false, true,
            false, NO_BUFFER_FLUSH, BrokerConstants.INFLIGHT_WINDOW_SIZE,
            DISABLED_TOPIC_ALIAS);
        // Overwrite the existing connection with new with topic alias disabled
        createMQTTConnection(config);

        connectMqtt5AndVerifyAck(sut);

        MqttPublishMessage publish = createPublishWithTopicNameAndTopicAlias("kitchen/blinds", 10);

        // Exercise
        PostOffice.RouteResult pubResult = sut.processPublish(publish);

        // Verify
        verifyConnectionIsDropped(pubResult, MqttReasonCodes.Disconnect.PROTOCOL_ERROR);
    }

    private void verifyConnectionIsDropped(PostOffice.RouteResult pubResult, MqttReasonCodes.Disconnect protocolError) {
        assertNotNull(pubResult);
        assertFalse(pubResult.isSuccess());
        assertFalse(channel.isOpen(), "Connection should be closed by the broker");
        // read last message sent
        MqttMessage disconnectMsg = channel.readOutbound();
        assertEquals(MqttMessageType.DISCONNECT, disconnectMsg.fixedHeader().messageType());
        assertEquals(protocolError.byteValue(), ((MqttReasonCodeAndPropertiesVariableHeader) disconnectMsg.variableHeader()).reasonCode());
    }

    private static void verifyNotContainsProperty(MqttPublishVariableHeader header, MqttProperties.MqttPropertyType typeToVerify) {
        Optional<? extends MqttProperties.MqttProperty> match = header.properties().listAll().stream()
            .filter(mp -> mp.propertyId() == typeToVerify.value())
            .findFirst();
        assertFalse(match.isPresent(), "Found a property of type " + typeToVerify);
    }

    private MqttPublishMessage createPublishWithTopicNameAndTopicAlias(String topicName, int topicAlias) {
        final MqttProperties propertiesWithTopicAlias = new MqttProperties();
        propertiesWithTopicAlias.add(
            new MqttProperties.IntegerProperty(
                MqttProperties.MqttPropertyType.TOPIC_ALIAS.value(), topicAlias));

        return MqttMessageBuilders.publish()
            .topicName(topicName)
            .properties(propertiesWithTopicAlias)
            .qos(MqttQoS.AT_MOST_ONCE)
            .payload(payload)
            .build();
    }

    private MqttPublishMessage createPublishWithTopicNameAndTopicAlias(int topicAlias) {
        final MqttProperties propertiesWithTopicAlias = new MqttProperties();
        propertiesWithTopicAlias.add(
            new MqttProperties.IntegerProperty(
                MqttProperties.MqttPropertyType.TOPIC_ALIAS.value(), topicAlias));

        return MqttMessageBuilders.publish()
            .properties(propertiesWithTopicAlias)
            .qos(MqttQoS.AT_MOST_ONCE)
            .payload(payload)
            .build();
    }

    private void connectMqtt5AndVerifyAck(MQTTConnection mqttConnection) {
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_5)
            .clientId(null)
            .cleanSession(true)
            .build();
        PostOffice.RouteResult connectResult = mqttConnection.processConnect(connect);
        assertNotNull(connectResult);
        assertTrue(connectResult.isSuccess());
        // given that CONN is processed by session event loop and from that is sent also the CONNACK, wait for
        // connection to be active.
        Awaitility.await()
            .atMost(Duration.ofSeconds(1))
            .until(mqttConnection::isConnected);
        MqttConnAckMessage connAckMsg = channel.readOutbound();
        assertEquals(MqttMessageType.CONNACK, connAckMsg.fixedHeader().messageType());
    }
}

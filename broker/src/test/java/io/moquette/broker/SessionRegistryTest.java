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
import io.moquette.persistence.EnqueuedMessageValueType;
import io.moquette.broker.security.PermitAllAuthorizatorPolicy;
import io.moquette.broker.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.broker.subscriptions.ISubscriptionsDirectory;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.moquette.broker.MQTTConnectionPublishTest.memorySessionsRepository;
import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static io.moquette.broker.NettyChannelAssertions.assertEqualsConnAck;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SessionRegistryTest {

    public static final boolean ANY_BOOLEAN = false;
    static final String FAKE_CLIENT_ID = "FAKE_123";
    static final String TEST_USER = "fakeuser";
    static final String TEST_PWD = "fakepwd";

    private MQTTConnection connection;
    private EmbeddedChannel channel;
    private SessionRegistry sut;
    private MqttMessageBuilders.ConnectBuilder connMsg;
    private static final BrokerConfiguration ALLOW_ANONYMOUS_AND_ZEROBYTE_CLIENT_ID =
        new BrokerConfiguration(true, true, false, NO_BUFFER_FLUSH);
    private MemoryQueueRepository queueRepository;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    public void setUp() {
        connMsg = MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_3_1).cleanSession(true);

        scheduler = Executors.newScheduledThreadPool(1);

        createMQTTConnection(ALLOW_ANONYMOUS_AND_ZEROBYTE_CLIENT_ID);
    }

    @AfterEach
    public void tearDown() {
        scheduler.shutdown();
    }

    private void createMQTTConnection(BrokerConfiguration config) {
        channel = new EmbeddedChannel();
        connection = createMQTTConnection(config, channel);
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
        sut = new SessionRegistry(subscriptions, memorySessionsRepository(), queueRepository, permitAll, scheduler);
        final PostOffice postOffice = new PostOffice(subscriptions,
            new MemoryRetainedRepository(), sut, ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, permitAll, loopsGroup);
        return new MQTTConnection(channel, config, mockAuthenticator, sut, postOffice);
    }

    @Test
    public void testConnAckContainsSessionPresentFlag() {
        System.out.println("testConnAckContainsSessionPresentFlag invoked");
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID)
                                        .protocolVersion(MqttVersion.MQTT_3_1_1)
                                        .build();
        NettyUtils.clientID(channel, FAKE_CLIENT_ID);
        NettyUtils.cleanSession(channel, false);

        // Connect a first time
        final SessionRegistry.SessionCreationResult res = sut.createOrReopenSession(msg, FAKE_CLIENT_ID, connection.getUsername());
        // disconnect
        res.session.disconnect();
//        sut.disconnect(FAKE_CLIENT_ID);

        // Exercise, reconnect
        EmbeddedChannel anotherChannel = new EmbeddedChannel();
        MQTTConnection anotherConnection = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZEROBYTE_CLIENT_ID, anotherChannel);
        final SessionRegistry.SessionCreationResult result = sut.createOrReopenSession(msg, FAKE_CLIENT_ID, anotherConnection.getUsername());

        // Verify
        assertEquals(SessionRegistry.CreationModeEnum.CREATED_CLEAN_NEW, result.mode);
        assertTrue(anotherChannel.isOpen(), "Connection is accepted and therefore should remain open");
    }

    @Test
    public void connectWithCleanSessionUpdateClientSession() throws ExecutionException, InterruptedException {
        // first connect with clean session true
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID).cleanSession(true).build();
        connection.processConnect(msg).completableFuture().get();
        assertEqualsConnAck(CONNECTION_ACCEPTED, channel.readOutbound());
        connection.processDisconnect(null).completableFuture().get();
        assertFalse(channel.isOpen());

        // second connect with clean session false
        EmbeddedChannel anotherChannel = new EmbeddedChannel();
        MQTTConnection anotherConnection = createMQTTConnection(ALLOW_ANONYMOUS_AND_ZEROBYTE_CLIENT_ID,
                                                                anotherChannel);
        MqttConnectMessage secondConnMsg = MqttMessageBuilders.connect()
            .clientId(FAKE_CLIENT_ID)
            .protocolVersion(MqttVersion.MQTT_3_1)
            .build();

        anotherConnection.processConnect(secondConnMsg).completableFuture().get();
        assertEqualsConnAck(CONNECTION_ACCEPTED, anotherChannel.readOutbound());

        // Verify client session is clean false
        Session session = sut.retrieve(FAKE_CLIENT_ID);
        assertFalse(session.isClean());
    }

    @Test
    public void testDropSessionWithNullClientId() {
        assertFalse(sut.dropSession(null, ANY_BOOLEAN), "Can't be successful when null clientId is passed");
    }

    @Test
    public void testDropSessionWithNotExistingClientId() {
        assertFalse(sut.dropSession(FAKE_CLIENT_ID, ANY_BOOLEAN), "Can't be successful when non existing clientId is passed");
    }

    @Test
    public void testDropSessionToForceClosingConnectedSessionWithoutCleaning() throws ExecutionException, InterruptedException {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID)
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        connection.processConnect(msg).completableFuture().get();
        assertEqualsConnAck(CONNECTION_ACCEPTED, channel.readOutbound());

        // Exercise
        assertTrue(sut.dropSession(FAKE_CLIENT_ID, false), "Operation must have successfully terminated");

        // Verify
        final Session stillStoredSession = sut.retrieve(FAKE_CLIENT_ID);
        assertTrue(stillStoredSession.disconnected(), "session is still present and disconnected");
    }

    @Test
    public void testDropSessionToForceClosingConnectedSessionWithCleaning() throws ExecutionException, InterruptedException {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID)
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();
        connection.processConnect(msg).completableFuture().get();
        assertEqualsConnAck(CONNECTION_ACCEPTED, channel.readOutbound());

        // Exercise
        assertTrue(sut.dropSession(FAKE_CLIENT_ID, true), "Operation must have successfully terminated");

        // Verify
        assertNull(sut.retrieve(FAKE_CLIENT_ID), "Session state can't be present");
    }

    @Test
    public void testSerializabilityOfPublishedMessage() {
        MVStore mvStore = new MVStore.Builder()
            .fileName(BrokerConstants.DEFAULT_PERSISTENT_PATH)
            .autoCommitDisabled()
            .open();
        final MVMap.Builder<String, SessionRegistry.PublishedMessage> builder =
            new MVMap.Builder<String, SessionRegistry.PublishedMessage>()
                .valueType(new EnqueuedMessageValueType());

        final ByteBuf payload = Unpooled.wrappedBuffer("Hello World!".getBytes(StandardCharsets.UTF_8));
        SessionRegistry.PublishedMessage msg = new SessionRegistry.PublishedMessage(Topic.asTopic("/say"),
            MqttQoS.AT_LEAST_ONCE, payload, false);
        try {
            // store a message in the MVStore
            final String mapName = "test_map";
            MVMap<String, SessionRegistry.PublishedMessage> persistentMap = mvStore.openMap(mapName, builder);
            String key = "message";
            persistentMap.put(key, msg);
            mvStore.close();

            // reopen the MVStore and read it
            mvStore = new MVStore.Builder()
                .fileName(BrokerConstants.DEFAULT_PERSISTENT_PATH)
                .autoCommitDisabled()
                .open();
            final SessionRegistry.PublishedMessage reloadedMsg = mvStore.openMap(mapName, builder).get(key);

            // Verify
            assertEquals("/say", reloadedMsg.topic.toString());
        } finally {
            mvStore.close();
            File dbFile = new File(BrokerConstants.DEFAULT_PERSISTENT_PATH);
            if (dbFile.exists()) {
                dbFile.delete();
            }
            assertFalse(dbFile.exists());
        }
    }
}

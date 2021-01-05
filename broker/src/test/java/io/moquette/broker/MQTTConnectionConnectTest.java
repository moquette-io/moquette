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
import io.moquette.broker.security.IAuthenticator;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.moquette.broker.NettyChannelAssertions.assertEqualsConnAck;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.*;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MQTTConnectionConnectTest {

    private static final String FAKE_CLIENT_ID = "FAKE_123";
    private static final String TEST_USER = "fakeuser";
    private static final String TEST_PWD = "fakepwd";
    private static final String EVIL_TEST_USER = "eviluser";
    private static final String EVIL_TEST_PWD = "unsecret";

    private MQTTConnection sut;
    private EmbeddedChannel channel;
    private SessionRegistry sessionRegistry;
    private MqttMessageBuilders.ConnectBuilder connMsg;
    private static final BrokerConfiguration CONFIG = new BrokerConfiguration(true, true, false, false);
    private IAuthenticator mockAuthenticator;
    private PostOffice postOffice;
    private MemoryQueueRepository queueRepository;

    @BeforeEach
    public void setUp() {
        connMsg = MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_3_1).cleanSession(true);

        mockAuthenticator = new MockAuthenticator(singleton(FAKE_CLIENT_ID), singletonMap(TEST_USER, TEST_PWD));

        ISubscriptionsDirectory subscriptions = new CTrieSubscriptionDirectory();
        ISubscriptionsRepository subscriptionsRepository = new MemorySubscriptionsRepository();
        subscriptions.init(subscriptionsRepository);
        queueRepository = new MemoryQueueRepository();

        final PermitAllAuthorizatorPolicy authorizatorPolicy = new PermitAllAuthorizatorPolicy();
        final Authorizator permitAll = new Authorizator(authorizatorPolicy);
        sessionRegistry = new SessionRegistry(subscriptions, queueRepository, permitAll);
        postOffice = new PostOffice(subscriptions, new MemoryRetainedRepository(), sessionRegistry,
                                    ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, permitAll);

        sut = createMQTTConnection(CONFIG);
        channel = (EmbeddedChannel) sut.channel;
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config) {
        EmbeddedChannel channel = new EmbeddedChannel();
        return createMQTTConnection(config, channel, postOffice);
    }

    private MQTTConnection createMQTTConnectionWithPostOffice(BrokerConfiguration config, PostOffice postOffice) {
        EmbeddedChannel channel = new EmbeddedChannel();
        return createMQTTConnection(config, channel, postOffice);
    }

    private MQTTConnection createMQTTConnection(BrokerConfiguration config, Channel channel, PostOffice postOffice) {
        return new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, postOffice);
    }

    @Test
    public void testZeroByteClientIdWithCleanSession() {
        // Connect message with clean session set to true and client id is null.
        MqttConnectMessage msg = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .clientId(null)
            .cleanSession(true)
            .build();

        sut.processConnect(msg);
        assertEqualsConnAck("Connection must be accepted", CONNECTION_ACCEPTED, channel.readOutbound());
        assertNotNull("unique clientid must be generated", sut.getClientId());
        assertTrue(sessionRegistry.retrieve(sut.getClientId()).isClean(), "clean session flag must be true");
        assertTrue(channel.isOpen(), "Connection must be open");
    }

    @Test
    public void invalidAuthentication() {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID)
            .username(TEST_USER + "_fake")
            .password(TEST_PWD)
            .build();

        // Exercise
        sut.processConnect(msg);

        // Verify
        assertEqualsConnAck(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, channel.readOutbound());
        assertFalse(channel.isOpen(), "Connection should be closed by the broker.");
    }

    @Test
    public void testConnect_badClientID() {
        connMsg.clientId("extremely_long_clientID_greater_than_23").build();

        // Exercise
        sut.processConnect(connMsg.clientId("extremely_long_clientID_greater_than_23").build());

        // Verify
        assertEqualsConnAck(CONNECTION_ACCEPTED, channel.readOutbound());
    }

    @Test
    public void testWillIsAccepted() {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID).willFlag(true)
            .willTopic("topic").willMessage("Topic message").build();

        // Exercise
        // m_handler.setMessaging(mockedMessaging);
        sut.processConnect(msg);

        // Verify
        assertEqualsConnAck(CONNECTION_ACCEPTED, channel.readOutbound());
        assertTrue(channel.isOpen(), "Connection is accepted and therefore should remain open");
    }

    @Test
    public void testWillIsFired() {
        final PostOffice postOfficeMock = mock(PostOffice.class);
        sut = createMQTTConnectionWithPostOffice(CONFIG, postOfficeMock);
        channel = (EmbeddedChannel) sut.channel;

        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID).willFlag(true)
            .willTopic("topic").willMessage("Topic message").build();
        sut.processConnect(msg);

        // Exercise
        sut.handleConnectionLost();

        // Verify
        verify(postOfficeMock).fireWill(any(Session.Will.class));
        assertFalse(sut.isConnected(), "Connection MUST be disconnected");
    }

    @Test
    public void acceptAnonymousClient() {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID).build();

        // Exercise
        sut.processConnect(msg);

        // Verify
        assertEqualsConnAck(CONNECTION_ACCEPTED, channel.readOutbound());
        assertTrue(channel.isOpen(), "Connection is accepted and therefore must remain open");
    }

    @Test
    public void validAuthentication() {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID)
            .username(TEST_USER).password(TEST_PWD).build();

        // Exercise
        sut.processConnect(msg);

        // Verify
        assertEqualsConnAck(CONNECTION_ACCEPTED, channel.readOutbound());
        assertTrue(channel.isOpen(), "Connection is accepted and therefore must remain open");
    }

    @Test
    public void noPasswdAuthentication() {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID)
            .username(TEST_USER)
            .build();

        // Exercise
        sut.processConnect(msg);

        // Verify
        assertEqualsConnAck(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, channel.readOutbound());
        assertFalse(channel.isOpen(), "Connection must be closed by the broker");
    }

    @Test
    public void prohibitAnonymousClient() {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID).build();
        BrokerConfiguration config = new BrokerConfiguration(false, true, false, false);

        sut = createMQTTConnection(config);
        channel = (EmbeddedChannel) sut.channel;

        // Exercise
        sut.processConnect(msg);

        // Verify
        assertEqualsConnAck(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, channel.readOutbound());
        assertFalse(channel.isOpen(), "Connection must be closed by the broker");
    }

    @Test
    public void prohibitAnonymousClient_providingUsername() {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID)
            .username(TEST_USER + "_fake")
            .build();
        BrokerConfiguration config = new BrokerConfiguration(false, true, false, false);

        createMQTTConnection(config);

        // Exercise
        sut.processConnect(msg);

        // Verify
        assertEqualsConnAck(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, channel.readOutbound());
        assertFalse(channel.isOpen(), "Connection should be closed by the broker.");
    }

    @Test
    public void testZeroByteClientIdNotAllowed() {
        BrokerConfiguration config = new BrokerConfiguration(false, false, false, false);

        sut = createMQTTConnection(config);
        channel = (EmbeddedChannel) sut.channel;

        // Connect message with clean session set to true and client id is null.
        MqttConnectMessage msg = connMsg.clientId(null)
            .protocolVersion(MqttVersion.MQTT_3_1_1)
            .cleanSession(true)
            .build();

        sut.processConnect(msg);
        assertEqualsConnAck("Zero byte client identifiers are not allowed",
                            CONNECTION_REFUSED_IDENTIFIER_REJECTED, channel.readOutbound());
        assertFalse(channel.isOpen(), "Connection must closed");
    }

    @Test
    public void testZeroByteClientIdWithoutCleanSession() {
        // Allow zero byte client ids
        // Connect message without clean session set to true but client id is still null
        MqttConnectMessage msg = MqttMessageBuilders.connect().clientId(null).protocolVersion(MqttVersion.MQTT_3_1_1)
            .build();

        sut.processConnect(msg);
        assertEqualsConnAck("Identifier must be rejected due to having clean session set to false",
                            CONNECTION_REFUSED_IDENTIFIER_REJECTED, channel.readOutbound());
        assertFalse(channel.isOpen(), "Connection must be closed by the broker");
    }

    @Test
    public void testBindWithSameClientIDBadCredentialsDoesntDropExistingClient() {
        // Connect a client1
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID)
            .username(TEST_USER)
            .password(TEST_PWD)
            .build();
        sut.processConnect(msg);
        assertEqualsConnAck(CONNECTION_ACCEPTED, channel.readOutbound());

        // create another connect same clientID but with bad credentials
        MqttConnectMessage evilClientConnMsg = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_3_1)
            .clientId(FAKE_CLIENT_ID)
            .username(EVIL_TEST_USER)
            .password(EVIL_TEST_PWD)
            .build();

        EmbeddedChannel evilChannel = new EmbeddedChannel();

        // Exercise
        BrokerConfiguration config = new BrokerConfiguration(true, true, false, false);
        final MQTTConnection evilConnection = createMQTTConnection(config, evilChannel, postOffice);
        evilConnection.processConnect(evilClientConnMsg);

        // Verify
        // the evil client gets a not auth notification
        assertEqualsConnAck(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, evilChannel.readOutbound());
        // the good client remains connected
        assertTrue(channel.isOpen(), "Original connected client must remain connected");
        assertFalse(evilChannel.isOpen(), "Channel trying to connect with bad credentials must be closed");
    }

    @Test
    public void testForceClientDisconnection_issue116() {
        MqttConnectMessage msg = connMsg.clientId(FAKE_CLIENT_ID)
            .username(TEST_USER)
            .password(TEST_PWD)
            .build();
        sut.processConnect(msg);
        assertEqualsConnAck(CONNECTION_ACCEPTED, channel.readOutbound());

        // now create another connection and check the new one closes the older
        MQTTConnection anotherConnection = createMQTTConnection(CONFIG);
        anotherConnection.processConnect(msg);
        EmbeddedChannel anotherChannel = (EmbeddedChannel) anotherConnection.channel;
        assertEqualsConnAck(CONNECTION_ACCEPTED, anotherChannel.readOutbound());

        // Verify
        assertFalse(channel.isOpen(), "First 'FAKE_CLIENT_ID' channel MUST be closed by the broker");
        assertTrue(anotherChannel.isOpen(), "Second 'FAKE_CLIENT_ID' channel MUST be still open");
    }
}

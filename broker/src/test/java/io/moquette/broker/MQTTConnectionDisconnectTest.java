/*
 * Copyright (c) 2012-2026 The original author or authors
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
import io.moquette.broker.security.IAuthenticator;
import io.moquette.broker.security.PermitAllAuthorizatorPolicy;
import io.moquette.broker.subscriptions.CTrieSubscriptionDirectory;
import io.moquette.broker.subscriptions.ISubscriptionsDirectory;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.metrics.MetricsProvider;
import io.moquette.metrics.MetricsProviderNull;
import io.moquette.persistence.MemorySessionsRepository;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static io.moquette.broker.NettyChannelAssertions.assertEqualsConnAck;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MQTTConnectionDisconnectTest {

    private static final String CLIENT_ID = "mqtt5_disconnect_client";
    private static final String TEST_USER = "fakeuser";
    private static final String TEST_PWD = "fakepwd";
    private static final String TOPIC = "/test/expiry";
    private static final int GLOBAL_EXPIRY_SECONDS = 600;
    private static final BrokerConfiguration CONFIG =
        new BrokerConfiguration(true, true, false, NO_BUFFER_FLUSH);

    private ScheduledExecutorService scheduler;
    private SessionEventLoopGroup loopsGroup;
    private ISubscriptionsDirectory subscriptions;
    private ISessionsRepository sessionsRepository;
    private SessionRegistry sessionRegistry;
    private PostOffice postOffice;
    private IAuthenticator mockAuthenticator;
    private ForwardableClock slidingClock;
    private EmbeddedChannel channel;
    private MQTTConnection sut;

    @BeforeEach
    public void setUp() {
        scheduler = Executors.newScheduledThreadPool(1);
        slidingClock = new ForwardableClock(Clock.fixed(Instant.parse("2026-11-26T00:00:00Z"), ZoneOffset.UTC));
        mockAuthenticator = new MockAuthenticator(singleton(CLIENT_ID), singletonMap(TEST_USER, TEST_PWD));

        subscriptions = new CTrieSubscriptionDirectory();
        subscriptions.init(new MemorySubscriptionsRepository());
        MemoryQueueRepository queueRepository = new MemoryQueueRepository();

        final MetricsProvider mp = new MetricsProviderNull();
        final Authorizator permitAll = new Authorizator(new PermitAllAuthorizatorPolicy());
        loopsGroup = new SessionEventLoopGroup(ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, 1024, mp);
        sessionsRepository = new MemorySessionsRepository();
        sessionRegistry = new SessionRegistry(subscriptions, sessionsRepository, queueRepository, permitAll,
            scheduler, slidingClock, GLOBAL_EXPIRY_SECONDS, loopsGroup, mp);
        postOffice = new PostOffice(subscriptions, new MemoryRetainedRepository(), sessionRegistry,
            sessionsRepository, ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, permitAll, loopsGroup, mp);

        newConnection();
    }

    @AfterEach
    public void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
        loopsGroup.terminate();
        scheduler.shutdown();
    }

    private void newConnection() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
        channel = new EmbeddedChannel();
        sut = new MQTTConnection(channel, CONFIG, mockAuthenticator, sessionRegistry, postOffice);
    }

    private MqttConnAckMessage connectMqtt5(boolean cleanStart, int sessionExpirySeconds) throws Exception {
        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(
            MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), sessionExpirySeconds));
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_5)
            .clientId(CLIENT_ID)
            .cleanSession(cleanStart)
            .properties(props)
            .build();
        sut.processConnect(connect).completableFuture().get(5, TimeUnit.SECONDS);
        MqttConnAckMessage connAck = channel.readOutbound();
        assertNotNull(connAck, "CONNACK must be sent");
        return connAck;
    }

    private void subscribe(String topic) throws Exception {
        MqttSubscribeMessage sub = MqttMessageBuilders.subscribe()
            .messageId(1)
            .addSubscription(MqttQoS.AT_LEAST_ONCE, topic)
            .build();
        sut.processSubscribe(sub).completableFuture().get(5, TimeUnit.SECONDS);
        channel.readOutbound();
    }

    private static MqttMessage buildMqtt5Disconnect(MqttReasonCodes.Disconnect reasonCode, Integer expirySeconds) {
        MqttProperties props = new MqttProperties();
        if (expirySeconds != null) {
            props.add(new MqttProperties.IntegerProperty(
                MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), expirySeconds));
        }
        return MqttMessageBuilders.disconnect()
            .reasonCode(reasonCode.byteValue())
            .properties(props)
            .build();
    }

    @Test
    public void givenPersistentSessionWhenDisconnectWithZeroExpiryThenSessionAndSubscriptionsArePurged()
            throws Exception {
        MqttConnAckMessage firstAck = connectMqtt5(false, 300);
        assertFalse(firstAck.variableHeader().isSessionPresent(), "Fresh session must not be present");
        subscribe(TOPIC);
        assertEquals(1, sessionsRepository.list().size(), "Persistent session must be stored");

        sut.processDisconnect(buildMqtt5Disconnect(MqttReasonCodes.Disconnect.NORMAL_DISCONNECT, 0))
            .completableFuture().get(5, TimeUnit.SECONDS);

        assertFalse(channel.isOpen(), "Channel must be closed on DISCONNECT");
        assertNull(sessionRegistry.retrieve(CLIENT_ID), "Session must be purged from registry");
        assertTrue(sessionsRepository.list().isEmpty(), "Session must be deleted from repository");
        List<Subscription> remainingSubs = subscriptions.matchWithoutQosSharpening(Topic.asTopic(TOPIC));
        assertTrue(remainingSubs.isEmpty(), "Session subscriptions must be removed on zero expiry DISCONNECT");

        newConnection();
        MqttConnAckMessage reconnectAck = connectMqtt5(false, 300);
        assertEqualsConnAck(MqttConnectReturnCode.CONNECTION_ACCEPTED, reconnectAck);
        assertFalse(reconnectAck.variableHeader().isSessionPresent(),
            "Session Present must be false after session was discarded on DISCONNECT");
    }

    @Test
    public void givenPersistentSessionWhenDisconnectWithUpdatedExpiryThenSessionExpiresAtNewInterval()
            throws Exception {
        connectMqtt5(false, 300);
        subscribe(TOPIC);

        sut.processDisconnect(buildMqtt5Disconnect(MqttReasonCodes.Disconnect.NORMAL_DISCONNECT, 5))
            .completableFuture().get(5, TimeUnit.SECONDS);

        Session retained = sessionRegistry.retrieve(CLIENT_ID);
        assertNotNull(retained, "Session with non-zero DISCONNECT expiry must remain active until expiry");
        assertEquals(5, retained.getSessionData().expiryInterval(), "Updated expiry interval must be stored");

        slidingClock.forward(Duration.ofSeconds(6));
        Awaitility.await()
            .atMost(3 * SessionRegistry.EXPIRED_SESSION_CLEANER_TASK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)
            .until(() -> sessionsRepository.list().isEmpty() && sessionRegistry.retrieve(CLIENT_ID) == null);
    }

    @Test
    public void givenZeroConnectExpiryWhenDisconnectContainsSessionExpiryThenBrokerSendsProtocolError()
            throws Exception {
        connectMqtt5(true, 0);

        PostOffice.RouteResult result = sut.processDisconnect(
            buildMqtt5Disconnect(MqttReasonCodes.Disconnect.NORMAL_DISCONNECT, 60));
        result.completableFuture().get(5, TimeUnit.SECONDS);

        MqttMessage outbound = channel.readOutbound();
        assertNotNull(outbound, "Broker must send DISCONNECT on [MQTT-3.14.2-2] violation");
        assertEquals(MqttMessageType.DISCONNECT, outbound.fixedHeader().messageType());
        MqttReasonCodeAndPropertiesVariableHeader header =
            (MqttReasonCodeAndPropertiesVariableHeader) outbound.variableHeader();
        assertEquals(MqttReasonCodes.Disconnect.PROTOCOL_ERROR.byteValue(), header.reasonCode(),
            "Broker must respond with PROTOCOL_ERROR (0x82)");
        assertFalse(channel.isOpen(), "Connection must be closed after PROTOCOL_ERROR");
        assertNull(sessionRegistry.retrieve(CLIENT_ID), "Zero-expiry session must still be purged");
        assertTrue(sessionsRepository.list().isEmpty(), "Zero-expiry session must not remain in repository");
    }

    @Test
    public void givenInfiniteSessionExpiryWhenDisconnectedThenIntervalIsClampedToGlobalMaxNotNegative()
            throws Exception {
        connectMqtt5(false, BrokerConstants.INFINITE_SESSION_EXPIRY);

        sut.processDisconnect(buildMqtt5Disconnect(
                MqttReasonCodes.Disconnect.NORMAL_DISCONNECT, BrokerConstants.INFINITE_SESSION_EXPIRY))
            .completableFuture().get(5, TimeUnit.SECONDS);

        Session retained = sessionRegistry.retrieve(CLIENT_ID);
        assertNotNull(retained, "Infinite session expiry must be clamped to positive global max");
        assertEquals(GLOBAL_EXPIRY_SECONDS, retained.getSessionData().expiryInterval());

        slidingClock.forward(Duration.ofSeconds(10));
        Thread.sleep(2 * SessionRegistry.EXPIRED_SESSION_CLEANER_TASK_INTERVAL.toMillis());
        assertNotNull(sessionRegistry.retrieve(CLIENT_ID), "Session must not prematurely expire at t=10s");

        slidingClock.forward(Duration.ofSeconds(GLOBAL_EXPIRY_SECONDS));
        Awaitility.await()
            .atMost(3 * SessionRegistry.EXPIRED_SESSION_CLEANER_TASK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)
            .until(() -> sessionsRepository.list().isEmpty() && sessionRegistry.retrieve(CLIENT_ID) == null);
    }

    @Test
    public void givenReopenedSessionWithZeroConnectExpiryWhenDisconnectedWithoutPropertyThenSessionIsPurged()
            throws Exception {
        connectMqtt5(false, 300);
        sut.processDisconnect(buildMqtt5Disconnect(MqttReasonCodes.Disconnect.NORMAL_DISCONNECT, null))
            .completableFuture().get(5, TimeUnit.SECONDS);
        assertNotNull(sessionRegistry.retrieve(CLIENT_ID), "Session must survive disconnect without expiry=0");

        newConnection();
        MqttConnAckMessage reconnectAck = connectMqtt5(false, 0);
        assertTrue(reconnectAck.variableHeader().isSessionPresent(), "Existing session must be reopened");

        sut.processDisconnect(buildMqtt5Disconnect(MqttReasonCodes.Disconnect.NORMAL_DISCONNECT, null))
            .completableFuture().get(5, TimeUnit.SECONDS);

        assertNull(sessionRegistry.retrieve(CLIENT_ID),
            "Session reopened with CONNECT expiry=0 must be purged on DISCONNECT");
        assertTrue(sessionsRepository.list().isEmpty(), "Purged session must be deleted from repository");
    }
}

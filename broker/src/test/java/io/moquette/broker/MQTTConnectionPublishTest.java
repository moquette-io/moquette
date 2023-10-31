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
import io.moquette.persistence.MemorySessionsRepository;
import io.moquette.persistence.MemorySubscriptionsRepository;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
//import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.moquette.BrokerConstants.NO_BUFFER_FLUSH;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class MQTTConnectionPublishTest {

    private static final String FAKE_CLIENT_ID = "FAKE_123";
    private static final String TEST_USER = "fakeuser";
    private static final String TEST_PWD = "fakepwd";

    private MQTTConnection sut;
    private EmbeddedChannel channel;
    private SessionRegistry sessionRegistry;
    private MqttMessageBuilders.ConnectBuilder connMsg;
    private MemoryQueueRepository queueRepository;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    public void setUp() {
        connMsg = MqttMessageBuilders.connect().protocolVersion(MqttVersion.MQTT_3_1).cleanSession(true);

        BrokerConfiguration config = new BrokerConfiguration(true, true, false, NO_BUFFER_FLUSH);

        scheduler = Executors.newScheduledThreadPool(1);

        createMQTTConnection(config);
    }

    @AfterEach
    public void tearDown() {
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
            new MemoryRetainedRepository(), sessionRegistry, fakeSessionRepo, ConnectionTestUtils.NO_OBSERVERS_INTERCEPTOR, permitAll, loopsGroup);
        return new MQTTConnection(channel, config, mockAuthenticator, sessionRegistry, postOffice);
    }

//    @NotNull
    static ISessionsRepository memorySessionsRepository() {
        return new MemorySessionsRepository();
    }

    @Test
    public void dropConnectionOnPublishWithInvalidTopicFormat() throws ExecutionException, InterruptedException {
        // Connect message with clean session set to true and client id is null.
        final ByteBuf payload = Unpooled.copiedBuffer("Hello MQTT world!".getBytes(UTF_8));
        MqttPublishMessage publish = MqttMessageBuilders.publish()
            .topicName("")
            .retained(false)
            .qos(MqttQoS.AT_MOST_ONCE)
            .payload(payload).build();

        sut.processPublish(publish).completableFuture().get();

        // Verify
        assertFalse(channel.isOpen(), "Connection should be closed by the broker");
        payload.release();
    }

}

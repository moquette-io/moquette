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
package io.moquette.integration;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.awaitility.core.ConditionTimeoutException;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ServerIntegrationRetainTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationRetainTest.class);

    private static IConfig m_config;
    private static Server m_server;

    private IMqttClient clientSubscriber;
    private IMqttClient clientPublisher;
    private MessageCollector callbackPublisher;
    private MessageCollector callbackSubscriber;

    @TempDir
    static Path tempFolder;

    private static void startServer(String dbPath) throws IOException {
        m_server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }

    @BeforeAll
    public static void beforeTests() throws IOException {
        Awaitility.setDefaultTimeout(Durations.FIVE_SECONDS);
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);
    }

    @AfterAll
    public static void afterTests() {
        m_server.stopServer();
        m_server = null;
        m_config = null;
    }

    @BeforeEach
    public void setUp() throws MqttException {
        final MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setCleanSession(true);

        clientSubscriber = new MqttClient("tcp://localhost:1883", "Subscriber", new MemoryPersistence());
        callbackSubscriber = new MessageCollector();
        clientSubscriber.setCallback(callbackSubscriber);
        clientSubscriber.connect(mqttConnectOptions);

        clientPublisher = new MqttClient("tcp://localhost:1883", "Publisher", new MemoryPersistence());
        callbackPublisher = new MessageCollector();
        clientPublisher.setCallback(callbackPublisher);
        clientPublisher.connect(mqttConnectOptions);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (clientPublisher.isConnected()) {
            clientPublisher.disconnect();
        }

        if (clientSubscriber.isConnected()) {
            clientSubscriber.disconnect();
        }
    }

    public void checkRetained(int qosPub, int qosSub, boolean shouldReceive) throws Exception {
        final String topic = "/topic" + qosPub + qosSub;
        final String messageString = "Hello world MQTT " + qosPub + " " + qosSub;
        clientPublisher.subscribe(topic);
        clientPublisher.publish(topic, messageString.getBytes(UTF_8), qosPub, true);
        // Wait for the publish to finish
        Awaitility.await().until(() -> callbackPublisher.isMessageReceived());
        callbackPublisher.getMessageImmediate();

        clientSubscriber.subscribe(topic, qosSub);
        try {
            Awaitility.await().until(() -> callbackSubscriber.isMessageReceived());
        } catch (ConditionTimeoutException ex) {
            // This may be fine.
        }
        final boolean messageReceived = callbackSubscriber.isMessageReceived();
        if (messageReceived) {
            MqttMessage message = callbackSubscriber.retrieveMessage();
            Assertions.assertTrue(shouldReceive, "QoS " + qosPub + " Messages should not be retained, received: " + message.toString());
            assertEquals(messageString, message.toString());
            assertEquals(Math.min(qosPub, qosSub), message.getQos());
        } else {
            Assertions.assertFalse(shouldReceive, "QoS " + qosPub + " Messages should be retained.");
        }
    }

    @Test
    public void checkSubscriberQoS0ReceiveQoS0Retained() throws Exception {
        LOG.info("*** checkSubscriberQoS0ReceiveQoS0Retained ***");
        checkRetained(0, 0, false);
    }

    @Test
    public void checkSubscriberQoS1ReceiveQoS0Retained() throws Exception {
        LOG.info("*** checkSubscriberQoS1ReceiveQoS0Retained ***");
        checkRetained(0, 1, false);
    }

    @Test
    public void checkSubscriberQoS2ReceiveQoS0Retained() throws Exception {
        LOG.info("*** checkSubscriberQoS2ReceiveQoS0Retained ***");
        checkRetained(0, 2, false);
    }

    @Test
    public void checkSubscriberQoS0ReceiveQoS1Retained() throws Exception {
        LOG.info("*** checkSubscriberQoS0ReceiveQoS1Retained ***");
        checkRetained(1, 0, true);
    }

    @Test
    public void checkSubscriberQoS1ReceiveQoS1Retained() throws Exception {
        LOG.info("*** checkSubscriberQoS1ReceiveQoS1Retained ***");
        checkRetained(1, 1, true);
    }

    @Test
    public void checkSubscriberQoS2ReceiveQoS1Retained() throws Exception {
        LOG.info("*** checkSubscriberQoS2ReceiveQoS1Retained ***");
        checkRetained(1, 2, true);
    }

    @Test
    public void checkSubscriberQoS0ReceiveQoS2Retained() throws Exception {
        LOG.info("*** checkSubscriberQoS0ReceiveQoS2Retained ***");
        checkRetained(2, 0, true);
    }

    @Test
    public void checkSubscriberQoS1ReceiveQoS2Retained() throws Exception {
        LOG.info("*** checkSubscriberQoS1ReceiveQoS2Retained ***");
        checkRetained(2, 1, true);
    }

    @Test
    public void checkSubscriberQoS2ReceiveQoS2Retained() throws Exception {
        LOG.info("*** checkSubscriberQoS2ReceiveQoS2Retained ***");
        checkRetained(2, 2, true);
    }

}

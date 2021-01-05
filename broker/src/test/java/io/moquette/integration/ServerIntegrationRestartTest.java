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
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ServerIntegrationRestartTest {

    static MqttConnectOptions CLEAN_SESSION_OPT = new MqttConnectOptions();

    Server m_server;
    IMqttClient m_subscriber;
    IMqttClient m_publisher;
    IConfig m_config;
    MessageCollector m_messageCollector;

    @TempDir
    Path tempFolder;
    private String dbPath;
    private MqttClientPersistence pubDataStore;
    private MqttClientPersistence subDataStore;

    protected void startServer(String dbPath) throws IOException {
        m_server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }

    @BeforeAll
    public static void beforeTests() {
        CLEAN_SESSION_OPT.setCleanSession(false);
        Awaitility.setDefaultTimeout(Durations.ONE_SECOND);
    }

    @BeforeEach
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);

        startServer(dbPath);

        pubDataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "publisher").getAbsolutePath());
        subDataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "subscriber").getAbsolutePath());
        m_subscriber = new MqttClient("tcp://localhost:1883", "Subscriber", subDataStore);
        m_messageCollector = new MessageCollector();
        m_subscriber.setCallback(m_messageCollector);

        m_publisher = new MqttClient("tcp://localhost:1883", "Publisher", pubDataStore);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (m_subscriber != null && m_subscriber.isConnected()) {
            m_subscriber.disconnect();
        }

        if (m_publisher != null && m_publisher.isConnected()) {
            m_publisher.disconnect();
        }

        m_server.stopServer();
    }

    @DisplayName("given not clean session then after a server restart the session should be present")
    @Test
    public void testNotCleanSessionIsVisibleAfterServerRestart() throws Exception {
        m_subscriber.connect(CLEAN_SESSION_OPT);
        m_subscriber.subscribe("/topic", 1);
        m_subscriber.disconnect();

        m_server.stopServer();
        m_server.startServer(IntegrationUtils.prepareTestProperties(dbPath));

        //publish a message
        m_publisher.connect();
        m_publisher.publish("/topic", "Hello world MQTT!!".getBytes(UTF_8), 1, false);

        //reconnect subscriber and topic should be sent
        m_subscriber.connect(CLEAN_SESSION_OPT);
        // verify the sent message while offline is read
        Awaitility.await().until(m_messageCollector::isMessageReceived);
        MqttMessage msg = m_messageCollector.retrieveMessage();
        assertEquals("Hello world MQTT!!", new String(msg.getPayload(), UTF_8));
    }

    @Test
    public void checkRestartCleanSubscriptionTree() throws Exception {
        // subscribe to /topic
        m_subscriber.connect(CLEAN_SESSION_OPT);
        m_subscriber.subscribe("/topic", 1);
        m_subscriber.disconnect();

        // shutdown the integration
        m_server.stopServer();

        // restart the integration
        m_server.startServer(IntegrationUtils.prepareTestProperties(dbPath));

        // reconnect the Subscriber subscribing to the same /topic but different QoS
        m_subscriber.connect(CLEAN_SESSION_OPT);
        m_subscriber.subscribe("/topic", 2);

        // should be just one registration so a publisher receive one notification
        m_publisher.connect(CLEAN_SESSION_OPT);
        m_publisher.publish("/topic", "Hello world MQTT!!".getBytes(UTF_8), 1, false);

        // read the messages
        Awaitility.await().until(m_messageCollector::isMessageReceived);
        MqttMessage msg = m_messageCollector.retrieveMessage();
        assertEquals("Hello world MQTT!!", new String(msg.getPayload(), UTF_8));
        Awaitility.await("no more messages on the same topic will be received")
            .during(Durations.ONE_SECOND)
            .atMost(Durations.TWO_SECONDS)
            .until(() -> !m_messageCollector.isMessageReceived());
    }

    @Test
    public void checkDontPublishInactiveClientsAfterServerRestart() throws Exception {
        IMqttClient conn = subscribeAndPublish("/topic");
        conn.disconnect();

        // shutdown the integration
        m_server.stopServer();

        // restart the integration
        m_server.startServer(IntegrationUtils.prepareTestProperties(dbPath));

        m_publisher.connect();
        m_publisher.publish("/topic", "Hello world MQTT!!".getBytes(UTF_8), 0, false);
    }

    @Test
    public void testClientDoesntRemainSubscribedAfterASubscriptionAndServerRestart() throws Exception {
        // subscribe to /topic
        m_subscriber.connect();
        // subscribe /topic
        m_subscriber.subscribe("/topic", 0);
        // unsubscribe from /topic
        m_subscriber.unsubscribe("/topic");
        m_subscriber.disconnect();

        // shutdown the integration
        m_server.stopServer();

        // restart the integration
        m_server.startServer(IntegrationUtils.prepareTestProperties(dbPath));
        // subscriber reconnects
        m_subscriber = new MqttClient("tcp://localhost:1883", "Subscriber", subDataStore);
        m_subscriber.setCallback(m_messageCollector);
        m_subscriber.connect();

        // publisher publishes on /topic
        m_publisher = new MqttClient("tcp://localhost:1883", "Publisher", pubDataStore);
        m_publisher.connect();
        m_publisher.publish("/topic", "Hello world MQTT!!".getBytes(UTF_8), 1, false);

        // Expected
        Awaitility.await("the subscriber doesn't get notified (it's fully unsubscribed)")
            .during(Durations.ONE_SECOND)
            .atMost(Durations.TWO_SECONDS)
            .until(() -> !m_messageCollector.isMessageReceived());
    }

    /**
     * Connect subscribe to topic and publish on the same topic
     */
    private IMqttClient subscribeAndPublish(String topic) throws Exception {
        IMqttClient client = new MqttClient("tcp://localhost:1883", "SubPub");
        MessageCollector collector = new MessageCollector();
        client.setCallback(collector);
        client.connect();
        client.subscribe(topic, 1);
        client.publish(topic, "Hello world MQTT!!".getBytes(UTF_8), 0, false);
        Awaitility.await().until(collector::isMessageReceived);
        MqttMessage msg = collector.retrieveMessage();
        assertEquals("Hello world MQTT!!", new String(msg.getPayload(), UTF_8));
        return client;
    }
}

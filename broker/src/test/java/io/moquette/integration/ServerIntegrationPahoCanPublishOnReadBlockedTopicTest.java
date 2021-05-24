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

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.AcceptAllAuthenticator;
import io.moquette.broker.security.IAuthorizatorPolicy;
import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Properties;

import static io.moquette.broker.ConnectionTestUtils.EMPTY_OBSERVERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

public class ServerIntegrationPahoCanPublishOnReadBlockedTopicTest {

    private static final Logger LOG =
        LoggerFactory.getLogger(ServerIntegrationPahoCanPublishOnReadBlockedTopicTest.class);

    Server m_server;
    IMqttClient m_client;
    IMqttClient m_publisher;
    MessageCollector m_messagesCollector;
    IConfig m_config;
    private boolean canRead;

    @TempDir
    Path tempFolder;

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.ONE_SECOND);
    }

    protected void startServer(String dbPath) {
        m_server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        configProps.setProperty(BrokerConstants.REAUTHORIZE_SUBSCRIPTIONS_ON_CONNECT, "true");
        m_config = new MemoryConfig(configProps);
        canRead = true;

        final IAuthorizatorPolicy switchingAuthorizator = new IAuthorizatorPolicy() {
//            int callCount = 0;
            @Override
            public boolean canWrite(Topic topic, String user, String client) {
                return true;
            }

            @Override
            public boolean canRead(Topic topic, String user, String client) {
                return canRead;
            }
        };

        m_server.startServer(m_config, EMPTY_OBSERVERS, null, new AcceptAllAuthenticator(), switchingAuthorizator);
    }

    @BeforeEach
    public void setUp() throws Exception {
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);

        MqttClientPersistence dataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder,"client").getAbsolutePath());
        MqttClientPersistence pubDataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder,"publisher").getAbsolutePath());

        m_client = new MqttClient("tcp://localhost:1883", "TestClient", dataStore);
        m_messagesCollector = new MessageCollector();
        m_client.setCallback(m_messagesCollector);

        m_publisher = new MqttClient("tcp://localhost:1883", "Publisher", pubDataStore);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (m_client != null && m_client.isConnected()) {
            m_client.disconnect();
        }

        if (m_publisher != null && m_publisher.isConnected()) {
            m_publisher.disconnect();
        }

        stopServer();
    }

    private void stopServer() {
        m_server.stopServer();
    }

    // TODO move this functional test into unit/integration
    @Test
    public void shouldNotInternalPublishOnReadBlockedSubscriptionTopic() throws Exception {
        LOG.info("*** shouldNotInternalPublishOnReadBlockedSubscriptionTopic ***");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);

        // Exercise
        MqttPublishMessage message = MqttMessageBuilders.publish()
            .topicName("/topic")
            .retained(true)
            .qos(MqttQoS.AT_MOST_ONCE)
            .payload(Unpooled.copiedBuffer("Hello World!!".getBytes(UTF_8)))
            .build();

        // We will be sending the same message again, retain the payload.
        message.payload().retain();
        m_server.internalPublish(message, "INTRLPUB");

        Awaitility.await().until(m_messagesCollector::isMessageReceived);
        final MqttMessage mqttMessage = m_messagesCollector.retrieveMessage();
        assertNotNull(mqttMessage);

        m_client.disconnect();
        // switch the authorizator
        canRead = false;

        // Exercise 2
        m_messagesCollector.reinit();
        m_client.connect(options);
        try {
            m_client.subscribe("/topic", 0);
            fail();
        } catch (MqttException mex) {
            // it's OK, the subscribed should fail with error code 128
        }

        m_server.internalPublish(message, "INTRLPUB");

        // verify the message is not published
        Awaitility.await("No message MUST be received")
            .during(Durations.ONE_SECOND)
            .atMost(Durations.TWO_SECONDS)
            .until(() -> !m_messagesCollector.isMessageReceived());
    }
}

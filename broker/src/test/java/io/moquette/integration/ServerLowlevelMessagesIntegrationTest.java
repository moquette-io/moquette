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

import static io.moquette.BrokerConstants.FLIGHT_BEFORE_RESEND_MS;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.awaitility.Awaitility;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

public class ServerLowlevelMessagesIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerLowlevelMessagesIntegrationTest.class);
    static MqttClientPersistence s_dataStore;
    Server m_server;
    Client m_client;
    IMqttClient m_willSubscriber;
    MessageCollector m_messageCollector;
    IConfig m_config;
    MqttMessage receivedMsg;

    @TempDir
    Path tempFolder;

    protected void startServer(String dbPath) throws IOException {
        m_server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.ONE_SECOND);
    }

    @BeforeEach
    public void setUp() throws Exception {
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);
        m_client = new Client("localhost");
        m_willSubscriber = new MqttClient("tcp://localhost:1883", "Subscriber", s_dataStore);
        m_messageCollector = new MessageCollector();
        m_willSubscriber.setCallback(m_messageCollector);
    }

    @AfterEach
    public void tearDown() throws Exception {
        m_client.close();
        Thread.sleep(300); // to let the close event pass before integration stop event
        m_server.stopServer();
    }

    @Test
    public void elapseKeepAliveTime() {
        int keepAlive = 2; // secs

        MqttConnectMessage connectMessage = createConnectMessage("FAKECLNT", keepAlive);

        /*
         * ConnectMessage connectMessage = new ConnectMessage();
         * connectMessage.setProtocolVersion((byte) 3); connectMessage.setClientID("FAKECLNT");
         * connectMessage.setKeepAlive(keepAlive);
         */
        m_client.sendMessage(connectMessage);

        // wait 3 times the keepAlive
        Awaitility.await()
            .atMost(3 * keepAlive, TimeUnit.SECONDS)
            .until(m_client::isConnectionLost);
    }

    private static MqttConnectMessage createConnectMessage(String clientID, int keepAlive) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE,
            false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader = new MqttConnectVariableHeader(
            MqttVersion.MQTT_3_1.protocolName(), MqttVersion.MQTT_3_1.protocolLevel(), false, false, false, 1, false,
            true, keepAlive);
        MqttConnectPayload mqttConnectPayload = new MqttConnectPayload(clientID, null, null,
                                                              null, (byte[]) null);
        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
    }

    @Test
    public void testWillMessageIsFiredOnClientKeepAliveExpiry() throws Exception {
        LOG.info("*** testWillMessageIsFiredOnClientKeepAliveExpiry ***");
        String willTestamentTopic = "/will/test";
        String willTestamentMsg = "Bye bye";

        m_willSubscriber.connect();
        m_willSubscriber.subscribe(willTestamentTopic, 0);

        m_client.clientId("FAKECLNT").connect(willTestamentTopic, willTestamentMsg);
        long connectTime = System.currentTimeMillis();

        Awaitility.await()
            .atMost(7, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                // but after the 2 KEEP ALIVE timeout expires it gets fired,
                // NB it's 1,5 * KEEP_ALIVE so 3 secs and some millis to propagate the message
                org.eclipse.paho.client.mqttv3.MqttMessage msg = m_messageCollector.getMessageImmediate();
                assertNotNull(msg, "will message should be fired after keep alive!");
                // the will message hasn't to be received before the elapsing of Keep Alive timeout
                assertTrue(System.currentTimeMillis() - connectTime > 3000);
                assertEquals(willTestamentMsg, new String(msg.getPayload(), UTF_8));
        });

        m_willSubscriber.disconnect();
    }

    @Test
    public void testRejectConnectWithEmptyClientID() throws InterruptedException {
        LOG.info("*** testRejectConnectWithEmptyClientID ***");
        this.receivedMsg = m_client.clientId("").connect();

        assertTrue(receivedMsg instanceof MqttConnAckMessage);
        MqttConnAckMessage connAck = (MqttConnAckMessage) receivedMsg;
        assertEquals(CONNECTION_REFUSED_IDENTIFIER_REJECTED, connAck.variableHeader().connectReturnCode());
    }

    @Test
    public void testWillMessageIsPublishedOnClientBadDisconnection() throws InterruptedException, MqttException {
        LOG.info("*** testWillMessageIsPublishedOnClientBadDisconnection ***");
        String willTestamentTopic = "/will/test";
        String willTestamentMsg = "Bye bye";
        m_willSubscriber.connect();
        m_willSubscriber.subscribe(willTestamentTopic, 0);
        m_client.clientId("FAKECLNT").connect(willTestamentTopic, willTestamentMsg);

        // kill will publisher
        m_client.close();

        // Verify will testament is published
        Awaitility.await().until(m_messageCollector::isMessageReceived);
        org.eclipse.paho.client.mqttv3.MqttMessage receivedTestament = m_messageCollector.retrieveMessage();
        assertEquals(willTestamentMsg, new String(receivedTestament.getPayload(), UTF_8));
        m_willSubscriber.disconnect();
    }

    @Test
    public void testResendNotAckedPublishes() throws MqttException, InterruptedException {
        LOG.info("*** testResendNotAckedPublishes ***");
        String topic = "/test";

        MqttClient subscriber = new MqttClient("tcp://localhost:1883", "Subscriber");
        MqttClient publisher = new MqttClient("tcp://localhost:1883", "Publisher");

        try {
            subscriber.connect();
            publisher.connect();

            AtomicBoolean isFirst = new AtomicBoolean(true);
            AtomicBoolean receivedPublish = new AtomicBoolean(false);
            subscriber.subscribe(topic, 1, (String topic1, org.eclipse.paho.client.mqttv3.MqttMessage message) -> {
                if (isFirst.getAndSet(false)) {
                    // wait to trigger resending PUBLISH
                    TimeUnit.MILLISECONDS.sleep(FLIGHT_BEFORE_RESEND_MS * 2);
                } else {
                    receivedPublish.set(true);
                }
            });

            publisher.publish(topic, "hello".getBytes(), 1, false);
            Awaitility.await("Waiting for resend.")
                .atMost(FLIGHT_BEFORE_RESEND_MS * 3, TimeUnit.MILLISECONDS)
                .pollDelay(FLIGHT_BEFORE_RESEND_MS * 2, TimeUnit.MILLISECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilTrue(receivedPublish);
        } finally {
            try {
                if (subscriber.isConnected()) {
                    subscriber.disconnect();
                }
            } finally {
                publisher.disconnect();
            }
        }
    }
}

/*
 * Copyright (c) 2012-2025 The original author or authors
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
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.interception.TopicRewriter;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;

public class ServerIntegrationTopicRewriteTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationPahoTest.class);

    private Server server;
    private IMqttClient client;
    private IMqttClient publisher;
    private MessageCollector messagesCollector;
    private IConfig config;

    @TempDir
    Path tempFolder;
    private String dbPath;

    protected void startServer(String dbPath) throws IOException {
        server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        server.setTopicRewriter(new TopicRewriter() {
            private final Pattern patternForward = Pattern.compile("^sensor/([^/]+)");
            private final Pattern patternInverse = Pattern.compile("^sensors/([^/]+)");

            @Override
            public Topic rewriteTopic(Subscription subscription) {
                Topic clientTopic = subscription.getTopicFilterClient();
                Matcher matcher = patternForward.matcher(clientTopic.toString());
                if (matcher.matches()) {
                    String internalTopic = "sensors/" + matcher.group(1);
                    LOG.info("Rewritten {} to {}", clientTopic, internalTopic);
                    return Topic.asTopic(internalTopic);
                }
                return clientTopic;
            }

            @Override
            public Topic rewriteTopicInverse(Topic clientTopic, Topic publishedTopicInternal) {
                Matcher matcher = patternInverse.matcher((CharSequence) publishedTopicInternal);
                if (matcher.matches()) {
                    String publishedTopicClient = "sensor/" + matcher.group(1);
                    LOG.info("Inverse-Rewritten {} to {}", publishedTopicInternal, publishedTopicClient);
                    return Topic.asTopic(publishedTopicClient);
                }
                return publishedTopicInternal;
            }
        });
        server.startServer(config);
    }

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.ONE_SECOND);
    }

    @BeforeEach
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);

        MqttClientPersistence dataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "client").getAbsolutePath());
        MqttClientPersistence pubDataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "publisher").getAbsolutePath());

        client = new MqttClient("tcp://localhost:1883", "TestClient", dataStore);
        messagesCollector = new MessageCollector();
        client.setCallback(messagesCollector);

        publisher = new MqttClient("tcp://localhost:1883", "Publisher", pubDataStore);
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationUtils.disconnectClient(client);
        IntegrationUtils.disconnectClient(publisher);

        stopServer();
    }

    private void stopServer() {
        server.stopServer();
    }

    /**
     * On the server the sensor sub-tree was moved to sensors, but legacy clients should still receive messages on the old topic.
     * Subscriber B subscribing to sensor/sensor1 should get messages sent to sensors/sensor1, as if they were sent to sensor/sensor1.
     * Subscriber C subscribing to sensor/+ should get messages sent to sensors/+, as if they were sent to sensor/+.
     */
    @Test
    public void checkSubscribersGetCorrectTopicNotifications() throws Exception {
        LOG.info("*** checkSubscribersGetCorrectTopicNotifications ***");

        MqttClientPersistence dsSubscriberA = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "subscriberA").getAbsolutePath());
        MqttClient subscriberA = new MqttClient("tcp://localhost:1883", "SubscriberA", dsSubscriberA);
        MessageCollector cbSubscriberA = new MessageCollector();
        subscriberA.setCallback(cbSubscriberA);
        subscriberA.connect();
        subscriberA.subscribe("sensors/sensor1", 1);

        MqttClientPersistence dsSubscriberB = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "subscriberB").getAbsolutePath());
        MqttClient subscriberB = new MqttClient("tcp://localhost:1883", "SubscriberB", dsSubscriberB);
        MessageCollector cbSubscriberB = new MessageCollector();
        subscriberB.setCallback(cbSubscriberB);
        subscriberB.connect();
        subscriberB.subscribe("sensor/sensor1", 2);

        MqttClientPersistence dsSubscriberC = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "subscriberC").getAbsolutePath());
        MqttClient subscriberC = new MqttClient("tcp://localhost:1883", "SubscriberC", dsSubscriberC);
        MessageCollector cbSubscriberC = new MessageCollector();
        subscriberC.setCallback(cbSubscriberC);
        subscriberC.connect();
        subscriberC.subscribe("sensor/+", 2);

        client.connect();
        final String messageText1 = "Hello world MQTT!!";
        client.publish("sensors/sensor1", messageText1.getBytes(UTF_8), 2, false);

        {
            Awaitility.await().until(cbSubscriberA::isMessageReceived);
            MqttMessage message = cbSubscriberA.retrieveMessage();
            assertNotNull(message, "MUST be a received message");
            assertEquals(messageText1, new String(message.getPayload(), UTF_8));
            assertEquals(1, message.getQos());
        }
        {
            Awaitility.await().until(cbSubscriberB::isMessageReceived);
            MqttMessage message = cbSubscriberB.retrieveMessage();
            assertNotNull(message, "MUST be a received message");
            assertEquals(messageText1, new String(message.getPayload(), UTF_8));
            assertEquals(2, message.getQos());
        }
        {
            Awaitility.await().until(cbSubscriberC::isMessageReceived);
            MqttMessage messageOnC = cbSubscriberC.retrieveMessage();
            assertNotNull(messageOnC, "MUST be a received message");
            assertEquals(messageText1, new String(messageOnC.getPayload(), UTF_8));
            assertEquals(2, messageOnC.getQos());
        }

        subscriberB.unsubscribe("sensor/sensor1");
        subscriberC.unsubscribe("sensor/+");

        final String messageText2 = "Hello world again";
        client.publish("sensors/sensor1", messageText2.getBytes(UTF_8), 2, false);

        {
            Awaitility.await().until(cbSubscriberA::isMessageReceived);
            MqttMessage message = cbSubscriberA.retrieveMessage();
            assertEquals(messageText2, new String(message.getPayload(), UTF_8));
            assertEquals(1, message.getQos());
            subscriberA.disconnect();
        }
        {
            Thread.sleep(Durations.ONE_SECOND.toMillis());
            assertFalse(cbSubscriberB.isMessageReceived(), "MUST NOT receive a message");
            subscriberB.disconnect();

            assertFalse(cbSubscriberC.isMessageReceived(), "MUST NOT receive a message");
            subscriberC.disconnect();
        }
    }

}

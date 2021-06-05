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
import io.netty.util.ResourceLeakDetector;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterAll;
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

// copied from ServerIntegrationRestartTest
public class ServerIntegrationNettyLeakDetectorTest {

    static MqttConnectOptions CLEAN_SESSION_OPT = new MqttConnectOptions();

    Server server;
    IMqttClient subscriber;
    IMqttAsyncClient publisher;
    IConfig config;
    MessageCollector messageCollector;

    @TempDir
    Path tempFolder;
    private String dbPath;
    private MqttClientPersistence pubDataStore;
    private MqttClientPersistence subDataStore;

    protected void startServer(String dbPath) throws IOException {
        server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        server.startServer(config);
    }

    @BeforeAll
    public static void beforeTests() {
        CLEAN_SESSION_OPT.setCleanSession(false);
        Awaitility.setDefaultTimeout(Durations.ONE_SECOND);
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterAll
    public static void afterTests() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
    }

    @BeforeEach
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);

        startServer(dbPath);

        pubDataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "publisher").getAbsolutePath());
        subDataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "subscriber").getAbsolutePath());
        subscriber = new MqttClient("tcp://localhost:1883", "Subscriber", subDataStore);
        messageCollector = new MessageCollector();
        subscriber.setCallback(messageCollector);

        publisher = new MqttAsyncClient("tcp://localhost:1883", "Publisher", pubDataStore);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (subscriber != null && subscriber.isConnected()) {
            subscriber.disconnect();
        }

        if (publisher != null && publisher.isConnected()) {
            publisher.disconnect();
        }

        server.stopServer();
    }

    @DisplayName("given not clean session then after a server restart the session should be present")
    @Test
    public void testNoLeakInNettyBuffers() throws Exception {
        subscriber.connect(CLEAN_SESSION_OPT);
        subscriber.subscribe("/topic", 1);

        publisher.connect().waitForCompletion();
        int sendBatchSize = 1000;
        for (int i = 0; i < sendBatchSize; i++) {
            if (i % 10 == 0) {
                // every 10 messages wait for completion
                waitForInflightPublishes(publisher);
                System.out.println("pub loop: " + i);
            }
            publisher.publish("/topic", ("Hello world MQTT " + i).getBytes(UTF_8), 1, false);
        }
        waitForInflightPublishes(publisher);

        publisher.disconnect().waitForCompletion();
        subscriber.disconnect();

        server.stopServer();

        Awaitility.await("the subscriber doesn't get notified of all the messages expected")
            .during(Durations.ONE_SECOND)
            .atMost(Durations.ONE_MINUTE)
            .until(() -> messageCollector.countReceived() == sendBatchSize);
    }

    private static void waitForInflightPublishes(IMqttAsyncClient publisher) throws MqttException, InterruptedException {
        for (IMqttDeliveryToken token : publisher.getPendingDeliveryTokens()) {
            token.waitForCompletion();
        }

        if (publisher.getPendingDeliveryTokens().length > 0) {
            System.out.println("After a wait still present events in flight: " + publisher.getPendingDeliveryTokens().length);
            if (publisher.getPendingDeliveryTokens().length > 8) {
                Thread.sleep(1000);
            }
        }
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

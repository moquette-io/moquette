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
package io.moquette.metrics;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import static io.moquette.broker.config.IConfig.METRICS_PROVIDER_CLASS;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.integration.IntegrationUtils;
import io.moquette.integration.MessageCollector;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.io.IOException;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.nio.file.Path;
import java.util.Properties;
import java.util.logging.Level;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the Prometheus Metrics Provider.
 */
public class MetricsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsTest.class.getName());

    private Server server;
    private IMqttClient clientListener;
    private IMqttClient clientPublisher;
    private MessageCollector messagesCollector;
    private IConfig config;

    @TempDir
    Path tempFolder;
    private String dbPath;

    protected void startServer(String dbPath) throws IOException {
        server = new Server();
        final Properties configProps = prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        server.startServer(config);
    }

    public static Properties prepareTestProperties(String dbPath) {
        Properties testProperties = IntegrationUtils.prepareTestProperties(dbPath);
        testProperties.put(METRICS_PROVIDER_CLASS, "MetricsProviderMock");
        return testProperties;
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

        clientListener = new MqttClient("tcp://localhost:1883", "TestClient", dataStore);
        messagesCollector = new MessageCollector();
        clientListener.setCallback(messagesCollector);

        clientPublisher = new MqttClient("tcp://localhost:1883", "Publisher", pubDataStore);
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationUtils.disconnectClient(clientListener);
        IntegrationUtils.disconnectClient(clientPublisher);

        stopServer();
    }

    private void stopServer() {
        server.stopServer();
    }

    @Test
    public void testMetrics() throws MqttException {
        MetricsProvider metricsProvider = server.getMetricsProvider();
        MetricsProviderMock mp = null;
        if (metricsProvider instanceof MetricsProviderMock) {
            mp = (MetricsProviderMock) metricsProvider;
        } else {
            Assertions.fail("MetricsProvider should be of type MetricsProviderMock, found " + metricsProvider.getClass());
        }

        assertEquals(0, mp.getSessionCount());
        assertEquals(0, mp.getPublishCount());
        assertEquals(0, mp.getMessageSum());
        assertEquals(0, mp.getSessionQueueFillSum());
        assertEquals(0, mp.getSessionQueueFillMax());
        assertEquals(0, mp.getSessionQueueOverrunSum());

        clientListener.connect();
        clientListener.subscribe("test/topic");
        clientPublisher.connect();
        clientPublisher.publish("test/topic", "Hello world MQTT!!".getBytes(UTF_8), 2, false);
        Awaitility.await().until(messagesCollector::isMessageReceived);

        assertEquals(2, mp.getSessionCount());
        assertEquals(1, mp.getPublishCount());
        assertEquals(1, mp.getMessageSum());
        assertEquals(0, mp.getMessageSum(MqttQoS.AT_MOST_ONCE.value()));
        assertEquals(1, mp.getMessageSum(MqttQoS.AT_LEAST_ONCE.value()));
        assertEquals(0, mp.getMessageSum(MqttQoS.EXACTLY_ONCE.value()));
        assertEquals(0, mp.getSessionQueueFillSum());
        assertTrue(mp.getSessionQueueFillMax() > 0);
        assertEquals(0, mp.getSessionQueueOverrunSum());
        mp.clearSessionQueueFillMax();

        clientListener.disconnect();
        clientPublisher.disconnect();

        try {
            // Sleep shortly to give the metrics time to update.
            Thread.sleep(100);
        } catch (InterruptedException ex) {
            // It's fine
        }

        assertEquals(0, mp.getSessionCount());
        assertEquals(1, mp.getPublishCount());
        assertEquals(1, mp.getMessageSum());
        assertEquals(0, mp.getMessageSum(MqttQoS.AT_MOST_ONCE.value()));
        assertEquals(1, mp.getMessageSum(MqttQoS.AT_LEAST_ONCE.value()));
        assertEquals(0, mp.getMessageSum(MqttQoS.EXACTLY_ONCE.value()));
        assertEquals(0, mp.getSessionQueueFillSum());
        assertTrue(mp.getSessionQueueFillMax() > 0);
        assertEquals(0, mp.getSessionQueueOverrunSum());
        mp.clearSessionQueueFillMax();

    }

}

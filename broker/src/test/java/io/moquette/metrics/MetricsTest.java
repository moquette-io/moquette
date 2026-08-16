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
import java.time.Duration;
import java.util.Properties;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.hamcrest.core.Is;
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

    private MetricsProviderMock validateMetricsProvider() {
        // Setup, check if the mock metrics provider is used and is clean.
        MetricsProvider metricsProvider = server.getMetricsProvider();
        assertTrue(metricsProvider instanceof MetricsProviderMock, "MetricsProvider should be of type MetricsProviderMock, found " + metricsProvider.getClass());
        MetricsProviderMock mp = (MetricsProviderMock) metricsProvider;
        return mp;
    }

    @Test
    public void testMetrics() throws MqttException {
        MetricsProviderMock mp = validateMetricsProvider();

        assertEquals(0, mp.getSessionCount(), "Incorrect value for metric 'SessionCount'");
        assertEquals(0, mp.getPublishCount(), "Incorrect value for metric 'PublishCount'");
        assertEquals(0, mp.getMessageSum(), "Incorrect value for metric 'MessageSum'");
        assertEquals(0, mp.getSessionQueueFillSum(), "Incorrect value for metric 'SessionQueueFillSum'");
        assertEquals(0, mp.getSessionQueueFillMax(), "Incorrect value for metric 'SessionQueueFillMax'");
        assertEquals(0, mp.getSessionQueueOverrunSum(), "Incorrect value for metric 'SessionQueueOverrunSum'");

        clientListener.connect();
        clientListener.subscribe("test/topic");
        clientPublisher.connect();
        clientPublisher.publish("test/topic", "Hello world MQTT!!".getBytes(UTF_8), 2, false);
        Awaitility.await().until(messagesCollector::isMessageReceived);

        assertEquals(2, mp.getSessionCount(), "Incorrect value for metric 'SessionCount'");
        assertEquals(1, mp.getPublishCount(), "Incorrect value for metric 'PublishCount'");
        assertEquals(1, mp.getMessageSum(), "Incorrect value for metric 'MessageSum'");
        assertEquals(0, mp.getMessageSum(MqttQoS.AT_MOST_ONCE.value()), "Incorrect value for metric 'MessageSum, QOS 0'");
        assertEquals(1, mp.getMessageSum(MqttQoS.AT_LEAST_ONCE.value()), "Incorrect value for metric 'MessageSum, QOS 1'");
        assertEquals(0, mp.getMessageSum(MqttQoS.EXACTLY_ONCE.value()), "Incorrect value for metric 'MessageSum, QOS 2'");
        assertEquals(0, mp.getSessionQueueFillSum(), "Incorrect value for metric 'SessionQueueFillSum'");
        assertTrue(mp.getSessionQueueFillMax() > 0, "Incorrect value for metric 'SessionQueueFillMax'");
        assertEquals(0, mp.getSessionQueueOverrunSum(), "Incorrect value for metric 'SessionQueueOverrunSum'");
        mp.clearSessionQueueFillMax();

        clientListener.disconnect();
        clientPublisher.disconnect();

        Awaitility.await("Wait for the metrics to update.")
            .pollInterval(Duration.ofMillis(10))
            .atMost(Duration.ofMillis(100))
            .until(() -> mp.getSessionCount(), Is.is(0));

        assertEquals(0, mp.getSessionCount(), "Incorrect value for metric 'SessionCount'");
        assertEquals(1, mp.getPublishCount(), "Incorrect value for metric 'PublishCount'");
        assertEquals(1, mp.getMessageSum(), "Incorrect value for metric 'MessageSum'");
        assertEquals(0, mp.getMessageSum(MqttQoS.AT_MOST_ONCE.value()), "Incorrect value for metric 'MessageSum, QOS 0'");
        assertEquals(1, mp.getMessageSum(MqttQoS.AT_LEAST_ONCE.value()), "Incorrect value for metric 'MessageSum, QOS 1'");
        assertEquals(0, mp.getMessageSum(MqttQoS.EXACTLY_ONCE.value()), "Incorrect value for metric 'MessageSum, QOS 2'");
        assertEquals(0, mp.getSessionQueueFillSum(), "Incorrect value for metric 'SessionQueueFillSum'");
        assertTrue(mp.getSessionQueueFillMax() > 0, "Incorrect value for metric 'SessionQueueFillMax'");
        assertEquals(0, mp.getSessionQueueOverrunSum(), "Incorrect value for metric 'SessionQueueOverrunSum'");
        mp.clearSessionQueueFillMax();
    }

    /**
     * Tests if replacing a non-clean session with a clean one correctly updates
     * the session count metric.
     *
     * @throws MqttException if there is a connection problem.
     */
    @Test
    public void givenClosedNonCleanSessionWhenReconnectAsCleanThenSessionCountMetricsDoesntMissIt() throws MqttException {
        // Setup, check if the mock metrics provider is used and is clean.
        MetricsProviderMock mp = validateMetricsProvider();
        assertEquals(0, mp.getSessionCount());

        // Connect two clients, one with clean session, one with non-clean session.
        final MqttConnectOptions listenerOptions = new MqttConnectOptions();
        listenerOptions.setCleanSession(false);
        clientListener.connect(listenerOptions);
        clientPublisher.connect();

        // There should now be two open sessions.
        Awaitility.await("Wait for the metrics to update.")
            .pollInterval(Duration.ofMillis(10))
            .atMost(Duration.ofMillis(100))
            .until(() -> mp.getSessionCount(), Is.is(2));

        // Disconnect the clients, this should leave the non-clean session open.
        clientListener.disconnect();
        clientPublisher.disconnect();

        // There should now be one open session, the non-clean listener session.
        Awaitility.await("Wait for the metrics to update.")
            .pollInterval(Duration.ofMillis(10))
            .atMost(Duration.ofMillis(100))
            .until(() -> mp.getSessionCount(), Is.is(1));

        // Re-open both sessions, but now both set to clean.
        listenerOptions.setCleanSession(true);
        clientListener.connect(listenerOptions);
        clientPublisher.connect();

        // There should now again be two open sessions, old non-clean session should be replaced with a clean one.
        Awaitility.await("Wait for the metrics to update.")
            .pollInterval(Duration.ofMillis(10))
            .atMost(Duration.ofMillis(100))
            .until(() -> mp.getSessionCount(), Is.is(2));

        // disconnect both sessions, this should leave 0 sessions open.
        clientListener.disconnect();
        clientPublisher.disconnect();

        // There should now be no open sessions.
        Awaitility.await("Wait for the metrics to update.")
            .pollInterval(Duration.ofMillis(10))
            .atMost(Duration.ofMillis(100))
            .until(() -> mp.getSessionCount(), Is.is(0));

    }

}

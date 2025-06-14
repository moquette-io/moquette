/*
 * Copyright (c) 2025 The original author or authors
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
package io.moquette.metrics.prometheus;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import static io.moquette.broker.config.IConfig.METRICS_PROVIDER_CLASS;
import io.moquette.broker.config.MemoryConfig;
import static io.moquette.metrics.prometheus.MetricsProviderPrometheus.METRIC_MOQUETTE_OPEN_SESSIONS;
import static io.moquette.metrics.prometheus.MetricsProviderPrometheus.METRIC_MOQUETTE_PUBLISHES_TOTAL;
import static io.moquette.metrics.prometheus.MetricsProviderPrometheus.METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL;
import static io.moquette.metrics.prometheus.MetricsProviderPrometheus.METRIC_MOQUETTE_SESSION_QUEUE_FILL;
import static io.moquette.metrics.prometheus.MetricsProviderPrometheus.METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX;
import static io.moquette.metrics.prometheus.MetricsProviderPrometheus.METRIC_MOQUETTE_SESSION_QUEUE_OVERRUNS_TOTAL;
import static io.moquette.metrics.prometheus.MetricsProviderPrometheus.TAG_ENDPOINT_PORT;
import java.io.IOException;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the Prometheus Metrics Provider.
 */
public class MetricsProviderPrometheusTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsProviderPrometheusTest.class.getName());

    Server m_server;
    IMqttClient clientListener;
    IMqttClient clientPublisher;
    MessageCollector messagesCollector;
    IConfig m_config;

    @TempDir
    Path tempFolder;
    private String dbPath;

    protected void startServer(String dbPath) throws IOException {
        m_server = new Server();
        final Properties configProps = prepareTestProperties(dbPath);
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }

    public static Properties prepareTestProperties(String dbPath) {
        Properties testProperties = IntegrationUtils.prepareTestProperties(dbPath);
        testProperties.put(METRICS_PROVIDER_CLASS, "MetricsProviderPrometheus");
        testProperties.put(TAG_ENDPOINT_PORT, "9400");
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
        m_server.stopServer();
    }

    @Test
    public void testMetrics() throws MqttException {
        String metricsUrl = "http://localhost:9400/metrics";
        HttpHelper.HttpResponse response = HttpHelper.doGet(metricsUrl);
        assertEquals(200, response.code, () -> "Error fetching metrics: " + metricsUrl);
        LOGGER.debug("Data: \n{}", response.response);
        Map<String, String> data = parseMetrics(response.response);
        assertEquals("0.0", data.get(METRIC_MOQUETTE_OPEN_SESSIONS));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_PUBLISHES_TOTAL));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL + "{QoS=\"0\",queue_name=\"queue-0\"}"));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL + "{QoS=\"1\",queue_name=\"queue-0\"}"));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL + "{QoS=\"2\",queue_name=\"queue-0\"}"));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL + "{queue_id=\"queue-0\"}"));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX + "{queue_id=\"queue-0\"}"));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_OVERRUNS_TOTAL + "{queue_name=\"queue-0\"}"));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX));

        clientListener.connect();
        clientListener.subscribe("test/topic");
        clientPublisher.connect();
        clientPublisher.publish("test/topic", "Hello world MQTT!!".getBytes(UTF_8), 2, false);
        Awaitility.await().until(messagesCollector::isMessageReceived);

        response = HttpHelper.doGet(metricsUrl);
        assertEquals(200, response.code, () -> "Error fetching metrics: " + metricsUrl);
        LOGGER.debug("Data: \n{}", response.response);
        data = parseMetrics(response.response);
        assertEquals("2.0", data.get(METRIC_MOQUETTE_OPEN_SESSIONS));
        assertEquals("1.0", data.get(METRIC_MOQUETTE_PUBLISHES_TOTAL));
        assertEquals("1.0", data.get(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL + "{queue_id=\"queue-0\"}"));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_OVERRUNS_TOTAL + "{queue_name=\"queue-0\"}"));
        assertTrue(Double.parseDouble(data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX)) > 0.0);

        clientListener.disconnect();
        clientPublisher.disconnect();

        response = HttpHelper.doGet(metricsUrl);
        assertEquals(200, response.code, () -> "Error fetching metrics: " + metricsUrl);
        LOGGER.debug("Data: \n{}", response.response);
        data = parseMetrics(response.response);
        assertEquals("0.0", data.get(METRIC_MOQUETTE_OPEN_SESSIONS));
        assertEquals("1.0", data.get(METRIC_MOQUETTE_PUBLISHES_TOTAL));
        assertEquals("1.0", data.get(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL + "{queue_id=\"queue-0\"}"));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_OVERRUNS_TOTAL + "{queue_name=\"queue-0\"}"));
        assertTrue(Double.parseDouble(data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX)) > 0.0);

        response = HttpHelper.doGet(metricsUrl);
        assertEquals(200, response.code, () -> "Error fetching metrics: " + metricsUrl);
        LOGGER.debug("Data: \n{}", response.response);
        data = parseMetrics(response.response);
        assertEquals("0.0", data.get(METRIC_MOQUETTE_OPEN_SESSIONS));
        assertEquals("1.0", data.get(METRIC_MOQUETTE_PUBLISHES_TOTAL));
        assertEquals("1.0", data.get(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL + "{queue_id=\"queue-0\"}"));
        assertEquals("0.0", data.get(METRIC_MOQUETTE_SESSION_QUEUE_OVERRUNS_TOTAL + "{queue_name=\"queue-0\"}"));
        assertTrue(Double.parseDouble(data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX)) == 0.0);

    }

    private Map<String, String> parseMetrics(String response) {
        Map<String, String> data = new HashMap<>();
        data.put(METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX, "0.0");
        data.put(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL, "0.0");
        response.lines().forEach(line -> {
            if (line.startsWith("#")) {
                return;
            }
            String[] split = line.split(" ", 2);
            if (split.length != 2 || split[0] == null || split[1] == null) {
                return;
            }
            data.put(split[0], split[1]);
            if (line.startsWith(METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX)) {
                data.put(METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX, Double.toString(Double.parseDouble(data.get(METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX)) + Double.parseDouble(split[1])));
            }
            if (line.startsWith(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL)) {
                data.put(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL, Double.toString(Double.parseDouble(data.get(METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL)) + Double.parseDouble(split[1])));
            }
        });
        return data;
    }

}

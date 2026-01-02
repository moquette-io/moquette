/*
 * Copyright (c) 2012-2022 The original author or authors
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
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.stream.Stream;
import org.awaitility.core.ConditionTimeoutException;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ServerIntegrationRetainTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationRetainTest.class);

    private static IConfig serverConfig;
    private static Server server;

    private IMqttClient clientSubscriber;
    private IMqttClient clientPublisher;
    private MessageCollector callbackPublisher;
    private MessageCollector callbackSubscriber;

    @TempDir
    static Path tempFolder;

    private static void startServer(String dbPath) throws IOException {
        server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        serverConfig = new MemoryConfig(configProps);
        server.startServer(serverConfig);
    }

    @BeforeAll
    public static void beforeTests() throws IOException {
        Awaitility.setDefaultTimeout(Durations.FIVE_SECONDS);
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);
    }

    @AfterAll
    public static void afterTests() {
        server.stopServer();
        server = null;
        serverConfig = null;
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

    private String createMessage(int qosPub, int qosSub) {
        return "Hello world MQTT " + qosPub + " " + qosSub;
    }

    private String createTopic(String testName, int qosPub, int qosSub) {
        return "/topic/" + testName + "/" + qosPub + qosSub;
    }

    private void sendRetainedAndSubscribe(String testName, int qosPub, int qosSub) throws MqttException {
        String topic = createTopic(testName, qosPub, qosSub);
        String messageString = createMessage(qosPub, qosSub);
        clientPublisher.subscribe(topic);
        callbackPublisher.reinit();
        clientPublisher.publish(topic, messageString.getBytes(UTF_8), qosPub, true);
        // Wait for the publish to finish
        Awaitility.await().until(() -> callbackPublisher.isMessageReceived());
        validateRetainedFlagNotSet(callbackPublisher.getMessageImmediate());

        callbackSubscriber.reinit();
        clientSubscriber.subscribe(topic, qosSub);
        try {
            Awaitility.await().until(() -> callbackSubscriber.isMessageReceived());
        } catch (ConditionTimeoutException ex) {
            // This may be fine.
        }
    }

    private void unsubscribeSubscriber(String testName, int qosPub, int qosSub) throws MqttException {
        String topic = createTopic(testName, qosPub, qosSub);
        clientSubscriber.unsubscribe(topic);
    }

    private void sendEmptyRetainedAndSubscribe(String testName, int qosPub, int qosSub) throws MqttException {
        String topic = createTopic(testName, qosPub, qosSub);
        callbackPublisher.reinit();
        clientPublisher.publish(topic, new byte[0], qosPub, true);
        // Wait for the publish to finish
        Awaitility.await().until(() -> callbackPublisher.isMessageReceived());
        validateRetainedFlagNotSet(callbackPublisher.getMessageImmediate());

        callbackSubscriber.reinit();
        clientSubscriber.subscribe(topic, qosSub);
        try {
            Awaitility.await().until(() -> callbackSubscriber.isMessageReceived());
        } catch (ConditionTimeoutException ex) {
            // This may be fine.
        }
    }

    private void validateRetainedFlagNotSet(MqttMessage message) {
        assertFalse(message.isRetained(), "Directly published version of messages should not have the retained flag set");
    }

    private void validateMustReceive(int qosPub, int qosSub) {
        final boolean messageReceived = callbackSubscriber.isMessageReceived();
        assertTrue(messageReceived, "Expected a message retained at QoS " + qosPub + ".");
        MqttMessage message = callbackSubscriber.getMessageImmediate();
        String expectedMessage = createMessage(qosPub, qosSub);
        assertEquals(expectedMessage, message.toString());
        assertEquals(Math.min(qosPub, qosSub), message.getQos());
    }

    private void validateMustNotReceive(int qosPub) {
        final boolean messageReceived = callbackSubscriber.isMessageReceived();
        MqttMessage message = callbackSubscriber.getMessageImmediate();
        assertFalse(messageReceived, "Received an unexpected message retained at QoS " + qosPub + ": " + message);
    }

    static Stream<Arguments> notRetainedProvider() {
        return Stream.of(
            arguments(0, 0),
            arguments(0, 1),
            arguments(0, 2)
        );
    }

    static Stream<Arguments> retainedProvider() {
        return Stream.of(
            arguments(1, 0),
            arguments(1, 1),
            arguments(1, 2),
            arguments(2, 0),
            arguments(2, 1),
            arguments(2, 2)
        );
    }

    @ParameterizedTest
    @MethodSource("notRetainedProvider")
    public void checkShouldNotRetain(int qosPub, int qosSub) throws MqttException {
        LOG.info("*** checkShouldNotRetain: qosPub {}, qosSub {} ***", qosPub, qosSub);
        sendRetainedAndSubscribe("should_not_retain", qosPub, qosSub);
        validateMustNotReceive(qosPub);
    }

    @ParameterizedTest
    @MethodSource("retainedProvider")
    public void checkShouldRetain(int qosPub, int qosSub) throws MqttException {
        LOG.info("*** checkShouldRetain: qosPub {}, qosSub {} ***", qosPub, qosSub);
        sendRetainedAndSubscribe("should_retain", qosPub, qosSub);
        validateMustReceive(qosPub, qosSub);
        unsubscribeSubscriber("should_retain", qosPub, qosSub);
        sendEmptyRetainedAndSubscribe("should_retain", qosPub, qosSub);
        validateMustNotReceive(qosPub);
    }

    @Test
    public void checkQos0CancelsRetain() throws MqttException {
        LOG.info("*** checkQos0CancelsRetain ***");
        // First send a QoS 2 retain, and check it arrives.
        sendRetainedAndSubscribe("qos0_cancel_retain", 2, 2);
        validateMustReceive(2, 2);
        unsubscribeSubscriber("qos0_cancel_retain", 2, 2);
        // Then send a QoS 0 retain, and check it cancels the previous retain.
        sendRetainedAndSubscribe("qos0_cancel_retain", 0, 2);
        validateMustNotReceive(0);
    }
}

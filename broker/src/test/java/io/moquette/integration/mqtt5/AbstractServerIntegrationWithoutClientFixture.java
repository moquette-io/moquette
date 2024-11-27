/*
 *
 *  * Copyright (c) 2012-2024 The original author or authors
 *  * ------------------------------------------------------
 *  * All rights reserved. This program and the accompanying materials
 *  * are made available under the terms of the Eclipse Public License v1.0
 *  * and Apache License v2.0 which accompanies this distribution.
 *  *
 *  * The Eclipse Public License is available at
 *  * http://www.eclipse.org/legal/epl-v10.html
 *  *
 *  * The Apache License v2.0 is available at
 *  * http://www.opensource.org/licenses/apache2.0.php
 *  *
 *  * You may elect to redistribute this code under either of these licenses.
 *
 */

package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.integration.IntegrationUtils;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AbstractServerIntegrationWithoutClientFixture {

    @TempDir
    Path tempFolder;
    protected String dbPath;
    Server broker;
    IConfig config;

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.ONE_SECOND);
    }

    static void verifyPayloadInUTF8(Mqtt5Publish msgPub, String expectedPayload) {
        assertTrue(msgPub.getPayload().isPresent(), "Response payload MUST be present");
        assertEquals(expectedPayload, new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8));
    }

    @BeforeEach
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);
    }

    @AfterEach
    public void tearDown() throws Exception {
        stopServer();
    }

    protected void startServer(String dbPath) throws IOException {
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        broker.startServer(config);
    }

    protected void startServer(IConfig config) throws IOException {
        broker = new Server();
        broker.startServer(config);
    }

    protected void stopServer() {
        broker.stopServer();
    }

    void restartServerWithSuspension(Duration timeout) throws InterruptedException, IOException {
        stopServer();
        Thread.sleep(timeout.toMillis());
        startServer(dbPath);
    }

    @NotNull
    static Mqtt5BlockingClient createHiveBlockingClient(String clientId) {
        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, client.connect().getReasonCode(), clientId + " connected");
        return client;
    }

    @NotNull
    static Mqtt5BlockingClient createHiveBlockingClientWithResponseProtocol(String clientId) {
        Mqtt5Connect connectRequest = Mqtt5Connect.builder()
            .keepAlive(10)
            .restrictions()
            .requestResponseInformation(true)
            .applyRestrictions()
            .build();

        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        Mqtt5ConnAck connAck = client.connect(connectRequest);
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, connAck.getReasonCode(), clientId + " connected");
        assertTrue(connAck.getResponseInformation().isPresent(), "ConnACK must contain response topic assigned by the broker");
        String responseTopic = connAck.getResponseInformation().get().toString();
        assertEquals(responseTopic, "/reqresp/response/" + clientId, "Response topic pattern MUST we respected");
        return client;
    }

    @NotNull
    static Mqtt5BlockingClient createPublisherClient() {
        return AbstractSubscriptionIntegrationTest.createClientWithStartFlagAndClientId(true, "publisher");
    }

    static void subscribeToAtQos1(Mqtt5BlockingClient subscriber, String topicFilter) {
        Mqtt5SubAck subAck = subscribe(subscriber, topicFilter, MqttQos.AT_LEAST_ONCE);
        assertThat(subAck.getReasonCodes()).contains(Mqtt5SubAckReasonCode.GRANTED_QOS_1);
    }

    static Mqtt5SubAck subscribe(Mqtt5BlockingClient subscriberClient, String topicFilter, MqttQos mqttQos) {
        return subscriberClient.subscribeWith()
            .topicFilter(topicFilter)
            .qos(mqttQos)
            .send();
    }

    static void verifyPublishSucceeded(Mqtt5PublishResult publishResult) {
        assertTrue(publishResult instanceof Mqtt5PublishResult.Mqtt5Qos1Result, "QoS1 Response must be present");
        Mqtt5PublishResult.Mqtt5Qos1Result qos1Result = (Mqtt5PublishResult.Mqtt5Qos1Result) publishResult;
        assertEquals(Mqtt5PubAckReasonCode.SUCCESS, qos1Result.getPubAck().getReasonCode(),
            "Publish can't be accepted by the broker");
    }

    protected static void verifyNoPublish(Mqtt5BlockingClient subscriber, Consumer<Void> action, Duration timeout, String message) throws InterruptedException {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            action.accept(null);
            Optional<Mqtt5Publish> publishedMessage = publishes.receive(timeout.getSeconds(), TimeUnit.SECONDS);

            // verify no published will in 10 seconds
            assertFalse(publishedMessage.isPresent(), message);
        }
    }

    protected static void verifyNoPublish(Mqtt5BlockingClient.Mqtt5Publishes publishes, Consumer<Void> action, Duration timeout, String message) throws InterruptedException {
        action.accept(null);
        Optional<Mqtt5Publish> publishedMessage = publishes.receive(timeout.getSeconds(), TimeUnit.SECONDS);

        // verify no published will in 10 seconds
        assertFalse(publishedMessage.isPresent(), message);
    }

    protected static void verifyPublishedMessage(Mqtt5BlockingClient.Mqtt5Publishes publishes, Consumer<Void> action, MqttQos expectedQos,
                                                 String expectedPayload, String errorMessage, int timeoutSeconds) throws Exception {
        action.accept(null);
        Optional<Mqtt5Publish> publishMessage = publishes.receive(timeoutSeconds, TimeUnit.SECONDS);
        if (!publishMessage.isPresent()) {
            fail("Expected to receive a publish message");
            return;
        }
        Mqtt5Publish msgPub = publishMessage.get();
        final String payload = new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8);
        assertEquals(expectedPayload, payload, errorMessage);
        assertEquals(expectedQos, msgPub.getQos());
    }

    static void verifyOfType(MqttMessage received, MqttMessageType mqttMessageType) {
        assertEquals(mqttMessageType, received.fixedHeader().messageType());
    }

    static void verifyPublishMessage(Mqtt5BlockingClient.Mqtt5Publishes publishListener, Consumer<Mqtt5Publish> assertion) throws InterruptedException {
        Optional<Mqtt5Publish> publishMessage = publishListener.receive(1, TimeUnit.SECONDS);
        if (!publishMessage.isPresent()) {
            fail("Expected to receive a publish message");
            return;
        }
        Mqtt5Publish msgPub = publishMessage.get();
        assertion.accept(msgPub);
    }
}

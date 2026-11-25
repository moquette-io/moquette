/*
 * Copyright (c) 2012-2026 The original author or authors
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
package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5DisconnectReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests verifying MQTT 5.0 DISCONNECT Session Expiry Interval handling.
 */
public class DisconnectSessionExpiryMqtt5Test extends AbstractServerIntegrationWithoutClientFixture {

    private static final String SUBSCRIBER_ID = "mqtt5-session-expiry-sub";
    private static final String TOPIC = "test/session-expiry";

    @Test
    public void givenPersistentSessionWhenDisconnectWithZeroExpiryThenSessionIsDiscarded() throws Exception {
        Mqtt5BlockingClient subscriber = buildHiveClient(SUBSCRIBER_ID);
        Mqtt5BlockingClient publisher = createHiveBlockingClient("publisher-zero-expiry");
        try {
            Mqtt5ConnAck initialAck = subscriber.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(300)
                .send();
            assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, initialAck.getReasonCode());
            assertFalse(initialAck.isSessionPresent(), "Initial session must not exist yet");

            subscriber.subscribeWith().topicFilter(TOPIC).qos(MqttQos.AT_LEAST_ONCE).send();
            waitUntilConnected(SUBSCRIBER_ID);

            subscriber.disconnectWith().sessionExpiryInterval(0).send();
            waitUntilDisconnected(SUBSCRIBER_ID);

            publisher.publishWith()
                .topic(TOPIC)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("while-offline".getBytes(StandardCharsets.UTF_8))
                .send();

            try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
                Mqtt5ConnAck reconnectAck = subscriber.connectWith()
                    .cleanStart(false)
                    .sessionExpiryInterval(300)
                    .send();
                assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, reconnectAck.getReasonCode());
                assertFalse(reconnectAck.isSessionPresent(),
                    "Session must be discarded after DISCONNECT with Session Expiry Interval = 0");

                Optional<Mqtt5Publish> unexpected = publishes.receive(500, TimeUnit.MILLISECONDS);
                assertFalse(unexpected.isPresent(),
                    "Offline message must not be delivered after session was discarded on DISCONNECT");
            }
        } finally {
            disconnectQuietly(subscriber);
            disconnectQuietly(publisher);
        }
    }

    @Test
    public void givenPersistentSessionWhenDisconnectWithoutExpiryOrUserPropertyOnlyThenSessionSurvives()
            throws Exception {
        Mqtt5BlockingClient subscriber = buildHiveClient(SUBSCRIBER_ID);
        Mqtt5BlockingClient publisher = createHiveBlockingClient("publisher-survives");
        try {
            subscriber.connectWith().cleanStart(false).sessionExpiryInterval(300).send();
            subscriber.subscribeWith().topicFilter(TOPIC).qos(MqttQos.AT_LEAST_ONCE).send();

            // 1. DISCONNECT with no properties keeps the session
            subscriber.disconnect();
            waitUntilDisconnected(SUBSCRIBER_ID);

            publisher.publishWith()
                .topic(TOPIC)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("offline-no-props".getBytes(StandardCharsets.UTF_8))
                .send();

            try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
                Mqtt5ConnAck reconnectAck = subscriber.connectWith()
                    .cleanStart(false)
                    .sessionExpiryInterval(300)
                    .send();
                assertTrue(reconnectAck.isSessionPresent(), "Session must survive DISCONNECT without expiry property");

                Mqtt5Publish received = publishes.receive(2, TimeUnit.SECONDS)
                    .orElseThrow(() -> new AssertionError("Expected queued QoS 1 message on reconnect"));
                verifyPayloadInUTF8(received, "offline-no-props");
            }

            // 2. DISCONNECT with User Property only (no Session Expiry Interval) also keeps the session
            subscriber.disconnectWith()
                .userProperties()
                    .add("reason", "reconnect-later")
                    .applyUserProperties()
                .send();
            waitUntilDisconnected(SUBSCRIBER_ID);

            publisher.publishWith()
                .topic(TOPIC)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("offline-user-prop".getBytes(StandardCharsets.UTF_8))
                .send();

            try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
                Mqtt5ConnAck reconnectAck = subscriber.connectWith()
                    .cleanStart(false)
                    .sessionExpiryInterval(300)
                    .send();
                assertTrue(reconnectAck.isSessionPresent(),
                    "Session must survive DISCONNECT carrying only User Property");

                Mqtt5Publish received = publishes.receive(2, TimeUnit.SECONDS)
                    .orElseThrow(() -> new AssertionError(
                        "Expected queued QoS 1 message after User Property disconnect"));
                verifyPayloadInUTF8(received, "offline-user-prop");
            }
        } finally {
            disconnectQuietly(subscriber);
            disconnectQuietly(publisher);
        }
    }

    @Test
    public void givenZeroConnectExpiryWhenDisconnectSendsSessionExpiryThenBrokerRespondsWithProtocolError()
            throws Exception {
        Client rawClient = new Client("localhost").clientId("raw-zero-expiry-client");
        try {
            MqttProperties connectProps = new MqttProperties();
            connectProps.add(new MqttProperties.IntegerProperty(
                MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), 0));
            MqttConnectMessage connect = MqttMessageBuilders.connect()
                .protocolVersion(MqttVersion.MQTT_5)
                .clientId("raw-zero-expiry-client")
                .cleanSession(true)
                .keepAlive(60)
                .properties(connectProps)
                .build();
            rawClient.sendMessage(connect);
            MqttMessage connectResp = rawClient.receiveNextMessage(Duration.ofSeconds(2));
            assertNotNull(connectResp, "CONNACK must be received");
            assertEquals(MqttMessageType.CONNACK, connectResp.fixedHeader().messageType());
            TestUtils.assertConnectionAccepted((MqttConnAckMessage) connectResp, "Initial CONNECT must succeed");

            MqttProperties disconnectProps = new MqttProperties();
            disconnectProps.add(new MqttProperties.IntegerProperty(
                MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), 60));
            MqttMessage invalidDisconnect = MqttMessageBuilders.disconnect()
                .reasonCode(MqttReasonCodes.Disconnect.NORMAL_DISCONNECT.byteValue())
                .properties(disconnectProps)
                .build();
            rawClient.sendMessage(invalidDisconnect);

            MqttMessage response = rawClient.receiveNextMessage(Duration.ofSeconds(2));
            assertNotNull(response, "Broker must send DISCONNECT with PROTOCOL_ERROR on [MQTT-3.14.2-2] violation");
            assertEquals(MqttMessageType.DISCONNECT, response.fixedHeader().messageType());
            MqttReasonCodeAndPropertiesVariableHeader header =
                (MqttReasonCodeAndPropertiesVariableHeader) response.variableHeader();
            assertEquals(MqttReasonCodes.Disconnect.PROTOCOL_ERROR.byteValue(), header.reasonCode());

            Awaitility.await("Channel must be closed after PROTOCOL_ERROR")
                .atMost(2, TimeUnit.SECONDS)
                .until(rawClient::isConnectionLost);
        } finally {
            rawClient.shutdownConnection();
        }
    }

    @Test
    public void givenDelayedWillWhenDisconnectWithWillAndZeroExpiryThenWillFiresImmediately() throws Exception {
        final String willTopic = "test/will/expiry";
        Mqtt5BlockingClient listener = createHiveBlockingClient("will-listener");
        Mqtt5BlockingClient clientWithWill = buildHiveClient("client-will-zero-expiry");
        try (Mqtt5BlockingClient.Mqtt5Publishes willPublishes = listener.publishes(MqttGlobalPublishFilter.ALL)) {
            listener.subscribeWith().topicFilter(willTopic).qos(MqttQos.AT_LEAST_ONCE).send();

            clientWithWill.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(300)
                .willPublish()
                    .topic(willTopic)
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .payload("goodbye-now".getBytes(StandardCharsets.UTF_8))
                    .delayInterval(60)
                    .applyWillPublish()
                .send();
            waitUntilConnected("client-will-zero-expiry");

            clientWithWill.disconnectWith()
                .reasonCode(Mqtt5DisconnectReasonCode.DISCONNECT_WITH_WILL_MESSAGE)
                .sessionExpiryInterval(0)
                .send();
            waitUntilDisconnected("client-will-zero-expiry");

            Mqtt5Publish willMsg = willPublishes.receive(3, TimeUnit.SECONDS)
                .orElseThrow(() -> new AssertionError(
                    "Will message must fire immediately when Session Expiry Interval is set to 0 on DISCONNECT"));
            verifyPayloadInUTF8(willMsg, "goodbye-now");

            Mqtt5ConnAck reconnectAck = clientWithWill.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(300)
                .send();
            assertFalse(reconnectAck.isSessionPresent(),
                "Session must remain discarded after Will-triggering DISCONNECT(0)");
        } finally {
            disconnectQuietly(clientWithWill);
            disconnectQuietly(listener);
        }
    }

    @Test
    public void givenDiscardedSessionByZeroDisconnectWhenBrokerRestartsThenSessionRemainsDiscarded()
            throws InterruptedException, IOException {
        Mqtt5BlockingClient subscriber = buildHiveClient(SUBSCRIBER_ID);
        try {
            subscriber.connectWith().cleanStart(false).sessionExpiryInterval(300).send();
            subscriber.subscribeWith().topicFilter(TOPIC).qos(MqttQos.AT_LEAST_ONCE).send();
            waitUntilConnected(SUBSCRIBER_ID);

            subscriber.disconnectWith().sessionExpiryInterval(0).send();
            waitUntilDisconnected(SUBSCRIBER_ID);

            restartServerWithSuspension(Duration.ofMillis(200));

            Mqtt5ConnAck afterRestartAck = subscriber.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(300)
                .send();
            assertFalse(afterRestartAck.isSessionPresent(),
                "Discarded session must not reappear after broker restart");
        } finally {
            disconnectQuietly(subscriber);
        }
    }

    private static Mqtt5BlockingClient buildHiveClient(String clientId) {
        return MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
    }

    private void waitUntilConnected(String clientId) {
        Awaitility.await("client connected")
            .atMost(Duration.ofSeconds(3))
            .until(() -> broker.listConnectedClients().stream()
                .anyMatch(c -> clientId.equals(c.getClientID())));
    }

    private void waitUntilDisconnected(String clientId) {
        Awaitility.await("client disconnected")
            .atMost(Duration.ofSeconds(3))
            .until(() -> broker.listConnectedClients().stream()
                .noneMatch(c -> clientId.equals(c.getClientID())));
    }

    private static void disconnectQuietly(Mqtt5BlockingClient client) {
        if (client != null && client.getState().isConnected()) {
            client.disconnect();
        }
    }
}

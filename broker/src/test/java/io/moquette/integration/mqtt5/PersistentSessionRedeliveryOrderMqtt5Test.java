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

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.moquette.integration.mqtt5.TestUtils.assertConnectionAccepted;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies, using MQTT 5 clients, that unacknowledged QoS1 messages of a persistent session are redelivered,
 * on reconnection, in the same order in which they were originally sent.
 */
public class PersistentSessionRedeliveryOrderMqtt5Test extends AbstractServerIntegrationTest {

    private static final String SUBSCRIBER_ID = "test_subscriber";
    private static final String TOPIC = "/test/persistent_session";
    private static final int MESSAGES_COUNT = 10;
    private static final Duration REDELIVERY_TIMEOUT = Duration.ofSeconds(5);

    @Override
    public String clientName() {
        return SUBSCRIBER_ID;
    }

    @Test
    public void givenPersistentSessionWhenReconnectsThenUnacknowledgedQoS1AreRedeliveredInOriginalOrder() throws Exception {
        verifyRedeliveryOrder(0);
    }

    /**
     * The broker resends the not acknowledged messages iterating a HashMap keyed by packet id. With small contiguous
     * ids (1..10) the iteration order is accidentally the ascending one. Acknowledging a first batch of messages
     * advances the packet ids, so that the ids of the not acknowledged ones (15..24) span a bucket boundary of the
     * 16 buckets map and the iteration order differs from the sending order.
     */
    @Test
    public void givenAdvancedPacketIdsWhenReconnectsThenUnacknowledgedQoS1AreRedeliveredInOriginalOrder() throws Exception {
        verifyRedeliveryOrder(14);
    }

    private void verifyRedeliveryOrder(int acknowledgedWarmUpMessages) throws Exception {
        // 1. subscriber connects and subscribes
        connectPersistentSubscriber(lowLevelClient);
        lowLevelClient.subscribe(TOPIC, MqttQoS.AT_LEAST_ONCE);

        // 2. publisher connects and publishes MESSAGES_COUNT messages at QoS1
        Mqtt5BlockingClient publisher = createPublisherClient();
        for (int i = 1; i <= acknowledgedWarmUpMessages; i++) {
            publisher.publishWith()
                .topic(TOPIC)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(("warmup-" + i).getBytes(StandardCharsets.UTF_8))
                .send();
            MqttPublishMessage warmUp = receivePublish(lowLevelClient, i, Duration.ofSeconds(5));
            assertEquals("warmup-" + i, payloadOf(warmUp));
            int warmUpPacketId = warmUp.variableHeader().packetId();
            warmUp.release();
            acknowledge(warmUpPacketId);
        }
        for (int i = 1; i <= MESSAGES_COUNT; i++) {
            publisher.publishWith()
                .topic(TOPIC)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(Integer.toString(i).getBytes(StandardCharsets.UTF_8))
                .send();
        }
        publisher.disconnect();

        // the subscriber receives all the messages but doesn't send any PUBACK
        for (int i = 1; i <= MESSAGES_COUNT; i++) {
            MqttPublishMessage publish = receivePublish(lowLevelClient, i, Duration.ofSeconds(5));
            assertEquals(Integer.toString(i), payloadOf(publish), "Messages are first delivered in order");
            publish.release();
        }

        // 3. subscriber disconnects without acknowledging
        lowLevelClient.disconnect();
        Awaitility.await("subscriber connection closed by broker")
            .atMost(Duration.ofSeconds(5))
            .until(() -> !lowLevelClient.isConnected());
        lowLevelClient.shutdownConnection();

        // 4. subscriber reconnects with the same client id, clean start = false
        lowLevelClient = new Client("localhost").clientId(SUBSCRIBER_ID);
        MqttConnAckMessage connAck = connectPersistentSubscriber(lowLevelClient);
        assertTrue(connAck.variableHeader().isSessionPresent(), "Session must be present on reconnection");

        // 5. messages are redelivered in the original order, this time they are acknowledged
        List<String> payloads = new ArrayList<>();
        for (int i = 1; i <= MESSAGES_COUNT; i++) {
            MqttPublishMessage publish = receivePublish(lowLevelClient, i, REDELIVERY_TIMEOUT);
            assertTrue(publish.fixedHeader().isDup(), "Redelivered message must have the DUP flag set");
            payloads.add(payloadOf(publish));
            int packetId = publish.variableHeader().packetId();
            publish.release();
            acknowledge(packetId);
        }

        final String[] expected = new String[MESSAGES_COUNT];
        for (int i = 0; i < MESSAGES_COUNT; i++) {
            expected[i] = Integer.toString(i + 1);
        }
        assertThat(payloads).containsExactly(expected);
    }

    private static MqttConnAckMessage connectPersistentSubscriber(Client client) throws InterruptedException {
        // MQTT5 session survives the disconnection only with a session expiry interval
        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(
            MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), 3600));
        MqttConnectMessage connect = MqttMessageBuilders.connect()
            .protocolVersion(MqttVersion.MQTT_5)
            .clientId(SUBSCRIBER_ID)
            .cleanSession(false)
            .keepAlive(60)
            .properties(props)
            .build();
        client.sendMessage(connect);
        MqttMessage msg = client.receiveNextMessage(Duration.ofSeconds(2));
        assertNotNull(msg, "CONNACK must be received");
        assertEquals(MqttMessageType.CONNACK, msg.fixedHeader().messageType());
        MqttConnAckMessage connAck = (MqttConnAckMessage) msg;
        assertConnectionAccepted(connAck, "Connection must be accepted");
        return connAck;
    }

    private static MqttPublishMessage receivePublish(Client client, int index, Duration timeout) throws InterruptedException {
        MqttMessage msg = client.receiveNextMessage(timeout);
        assertNotNull(msg, "PUBLISH #" + index + " must be received");
        assertEquals(MqttMessageType.PUBLISH, msg.fixedHeader().messageType());
        return (MqttPublishMessage) msg;
    }

    private static String payloadOf(MqttPublishMessage publish) {
        return publish.payload().toString(StandardCharsets.UTF_8);
    }
}

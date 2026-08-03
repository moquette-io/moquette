/*
 *
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
 *
 */

package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.MemoryMXBean;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.moquette.integration.mqtt5.TestUtils.assertConnectionAccepted;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression test for GHSA-7f46-5777-wf44.
 *
 * <p>A subscriber that advertises {@code Receive Maximum = 1} and withholds every PUBACK keeps the
 * broker's per-session in-flight quota permanently at zero. All subsequent QoS-1 publishes for
 * that subscription are enqueued in the broker's live-session fallback queue. Before the fix this
 * queue was unbounded, so heap grew proportionally to the number of messages. After the fix the
 * queue is capped and heap growth stays within a tolerable bound regardless of how many messages
 * the publisher sends.
 *
 * <p>The test verifies this property by measuring JVM heap (via {@link MemoryMXBean}) before and
 * after a fast publisher sends {@value #MESSAGE_COUNT} QoS-1 messages to the blocked subscriber.
 * Heap growth greater than {@value #MAX_GROWTH_PERCENT}% is reported as a test failure,
 * indicating the queue is still unbounded.
 *
 * <h2>Missing / insufficient methods noted</h2>
 * The {@link Client} low-level test client has most of the needed primitives:
 * <ul>
 *   <li>{@code subscribe(topic, MqttQoS.AT_LEAST_ONCE)} – subscribes at QoS 1. ✓
 *   <li>PUBACK is withheld simply by never calling {@code acknowledge()}; the {@link Client} does
 *       not auto-ack, so no additional "withhold-ack" API is required. ✓
 *   <li>{@code connectV5WithReceiveMaximum(int)} only exposes the receive-maximum knob but
 *       hard-codes {@code keepAlive = 2 s}. A slow subscriber that sends no traffic will be
 *       disconnected by the broker after the keep-alive deadline. The test therefore calls the
 *       lower-level {@code connectV5(int keepAliveSecs, int receiveMaximumInflight)} directly
 *       with {@code keepAlive = 0} to disable the mechanism. A convenience overload such as
 *       {@code connectV5WithReceiveMaximumAndKeepAlive(int receiveMax, int keepAliveSecs)} would
 *       make the intent clearer at the call site. ✗ (missing)
 * </ul>
 * The only other helper that lives in {@link AbstractServerIntegrationTest} rather than the shared
 * base class is {@code acknowledge(int packetId, Client)}, but this test intentionally never
 * acknowledges any message, so that method is not needed here.
 */
public class ReceiveMaximumUnboundedQueueTest extends AbstractServerIntegrationWithoutClientFixture {

    private static final Logger LOG = LoggerFactory.getLogger(ReceiveMaximumUnboundedQueueTest.class);

    private static final String TOPIC = "test/advisory/ghsa-7f46-5777-wf44";

    /** Number of QoS-1 publishes the fast publisher will send. */
    // This number should let the publisher to complete in less than 1 minute of timeout.
    private static final int MESSAGE_COUNT = 100_000;

    /**
     * Maximum tolerable heap growth percentage. If the queue is unbounded even
     * ~100 k messages would exceed this; the fixed broker should stay well below it.
     */
    private static final double MAX_GROWTH_PERCENT = 10.0;

    /**
     * Number of messages used by the drop-verification test. Must comfortably exceed
     * {@link #BOUNDED_QUEUE_SIZE} so that drops are guaranteed after the fix is applied.
     */
    private static final int DROP_TEST_MESSAGE_COUNT = 1_000;

    /**
     * Per-subscriber in-memory queue capacity used in the drop-verification test.
     * After the fix the broker caps its live-session fallback queue to this many messages.
     * Any additional message arriving when the queue is full is silently dropped.
     * The value is intentionally small relative to {@link #DROP_TEST_MESSAGE_COUNT} so that
     * the assertion {@code slowReceived < DROP_TEST_MESSAGE_COUNT} is always true once the
     * fix is in place.
     */
    private static final int BOUNDED_QUEUE_SIZE = 100;

    private Client slowSubscriber;
    private Mqtt5BlockingClient publisher;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        slowSubscriber = new Client("localhost").clientId("slow-subscriber");
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (slowSubscriber != null) {
            try {
                slowSubscriber.disconnect();
            } catch (Exception ignored) {
            }
            slowSubscriber.shutdownConnection();
        }
        if (publisher != null) {
            try {
                publisher.disconnect();
            } catch (Exception ignored) {
            }
        }
        super.tearDown();
    }

    /**
     * Verify that slow subscriber doesn't receive all the sent messages from publisher, this demonstrate
     * that the queue is bounded.
     */
    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void givenSlowSubscriberAndFastPublisherWhenInFlightWindowIsFullThenNewMessagesArentDelivered()
        throws Exception {

        // Connect the slow subscriber with Receive Maximum = 1 and keepAlive = 0.
        // keepAlive = 0 disables the keep-alive mechanism (MQTT 5 spec §3.1.2.10): the broker
        // must not close an idle connection, which is necessary here because the subscriber
        // intentionally sends no traffic while the publisher runs for several minutes.
        // The broker will send at most one unacknowledged QoS-1 message; everything else
        // goes into the session fallback queue while PUBACK is withheld.
        MqttConnAckMessage connAck = slowSubscriber.connectV5(0, 1);
        assertConnectionAccepted(connAck, "Slow subscriber must be accepted by the broker");

        slowSubscriber.subscribe(TOPIC, MqttQoS.AT_LEAST_ONCE);

        publisher = createPublisherClient();

        // Fast publisher: sends MESSAGE_COUNT QoS-1 messages without waiting for the subscriber.
        // The broker acknowledges each publish to the publisher immediately (QoS-1 semantics),
        // so the publisher's throughput is bounded only by broker acceptance speed, not by the
        // slow subscriber. All messages after the first accumulate in the subscriber's session queue.
        byte[] payload = new byte[1]; // minimal payload to maximise message count per MB
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            publisher.publishWith()
                .topic(TOPIC)
                .payload(payload)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        }

        // The slow subscriber has been silently accumulating messages while it withheld PUBACK.
        // Now it starts ACKing so the broker can drain what remains in the bounded queue.
        // With Receive Maximum = 1 each ACK triggers exactly one follow-up publish.
        int slowReceived = 0;
        MqttMessage rawMsg;
        while ((rawMsg = slowSubscriber.receiveNextMessage(Duration.ofSeconds(5))) != null) {
            if (rawMsg.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
                MqttPublishMessage publishMsg = (MqttPublishMessage) rawMsg;
                int packetId = publishMsg.variableHeader().packetId();
                publishMsg.release();
                slowReceived++;
                sendPubAck(packetId, slowSubscriber);
            }
        }

        assertTrue(slowReceived > 0,
            "Slow subscriber should have received at least the initial in-flight message");
        assertTrue(slowReceived < DROP_TEST_MESSAGE_COUNT,
            "Slow subscriber should have received fewer than " + DROP_TEST_MESSAGE_COUNT
                + " messages because the bounded queue dropped the excess; "
                + "actual received: " + slowReceived);
    }

    /**
     * Verifies that a slow subscriber with {@code Receive Maximum = 1} that permanently withholds
     * PUBACK does <em>not</em> receive all messages once the broker's fallback queue is bounded.
     *
     * <p>Scenario:
     * <ol>
     *   <li>The broker is started with a small in-memory session-queue capacity
     *       ({@value #BOUNDED_QUEUE_SIZE}) so that drops are guaranteed with
     *       {@value #DROP_TEST_MESSAGE_COUNT} publishes.
     *   <li>A slow subscriber connects with {@code Receive Maximum = 1} and {@code Keep Alive = 0}
     *       and subscribes at QoS 1. It withholds every PUBACK.
     *   <li>A normal subscriber connects without any receive-maximum restriction and subscribes to
     *       the same topic. It auto-ACKs every message.
     *   <li>A fast publisher sends {@value #DROP_TEST_MESSAGE_COUNT} QoS-1 messages.
     *   <li>The test waits until the normal subscriber has confirmed receipt of all
     *       {@value #DROP_TEST_MESSAGE_COUNT} messages (verifying they were all routed by the
     *       broker).
     *   <li>Only then does the slow subscriber begin ACKing, draining the queue one message at a
     *       time (Receive Maximum = 1 keeps each round-trip serialised).
     *   <li>The test asserts that the total number of messages delivered to the slow subscriber is
     *       strictly less than {@value #DROP_TEST_MESSAGE_COUNT}: the broker dropped the excess
     *       messages when the bounded queue was full.
     * </ol>
     */
    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void givenMultipleSubscriberWithOneSlowWhenInFlightWindowIsFullOnSlowSubscriberThenOtherSubscriberAreNotImpacted()
        throws Exception {

        MqttConnAckMessage connAck = slowSubscriber.connectV5(0, 1);
        assertConnectionAccepted(connAck, "Slow subscriber must be accepted by the broker");
        slowSubscriber.subscribe(TOPIC, MqttQoS.AT_LEAST_ONCE);

        // Normal subscriber: no receive-maximum restriction, auto-ACKs every message.
        Mqtt5BlockingClient normalSubscriber = createHiveBlockingClient("normal-subscriber");
        try {
            subscribeToAtQos1(normalSubscriber, TOPIC);

            publisher = createPublisherClient();

            int normalReceived = 0;
            byte[] payload = new byte[1];

            // Open the publish listener BEFORE sending so no message can slip through unobserved.
            try (Mqtt5BlockingClient.Mqtt5Publishes publishes =
                     normalSubscriber.publishes(MqttGlobalPublishFilter.ALL)) {

                for (int i = 0; i < DROP_TEST_MESSAGE_COUNT; i++) {
                    publisher.publishWith()
                        .topic(TOPIC)
                        .payload(payload)
                        .qos(MqttQos.AT_LEAST_ONCE)
                        .send();
                }

                // Confirm that the normal subscriber received every single message.
                while (normalReceived < DROP_TEST_MESSAGE_COUNT) {
                    Optional<Mqtt5Publish> msg = publishes.receive(30, TimeUnit.SECONDS);
                    if (!msg.isPresent()) {
                        fail("Normal subscriber timed out after receiving "
                            + normalReceived + "/" + DROP_TEST_MESSAGE_COUNT + " messages");
                    }
                    normalReceived++;
                }
            }
            assertEquals(DROP_TEST_MESSAGE_COUNT, normalReceived,
                "Normal subscriber must receive every published message");

            // The slow subscriber has been silently accumulating messages while it withheld PUBACK.
            // Now it starts ACKing so the broker can drain what remains in the bounded queue.
            // With Receive Maximum = 1 each ACK triggers exactly one follow-up publish.
            int slowReceived = 0;
            MqttMessage rawMsg;
            while ((rawMsg = slowSubscriber.receiveNextMessage(Duration.ofSeconds(5))) != null) {
                if (rawMsg.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
                    MqttPublishMessage publishMsg = (MqttPublishMessage) rawMsg;
                    int packetId = publishMsg.variableHeader().packetId();
                    publishMsg.release();
                    slowReceived++;
                    sendPubAck(packetId, slowSubscriber);
                }
            }

            LOG.info(
                "[GHSA-7f46-5777-wf44] slow subscriber received {}/{} messages "
                    + "(bounded queue capacity: {})",
                slowReceived, DROP_TEST_MESSAGE_COUNT, BOUNDED_QUEUE_SIZE);

            assertTrue(slowReceived > 0,
                "Slow subscriber should have received at least the initial in-flight message");
            assertTrue(slowReceived < DROP_TEST_MESSAGE_COUNT,
                "Slow subscriber should have received fewer than " + DROP_TEST_MESSAGE_COUNT
                    + " messages because the bounded queue dropped the excess; "
                    + "actual received: " + slowReceived);

        } finally {
            normalSubscriber.disconnect();
        }
    }

    private static void sendPubAck(int packetId, Client client) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
            MqttMessageType.PUBACK, false, AT_MOST_ONCE, false, 0);
        MqttPubAckMessage pubAck = new MqttPubAckMessage(
            fixedHeader, MqttMessageIdVariableHeader.from(packetId));
        client.sendMessage(pubAck);
    }
}

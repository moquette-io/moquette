/*
 * Copyright (c) 2012-2023 The original author or authors
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
package io.moquette.persistence;

import io.moquette.broker.SessionMessageQueue;
import io.moquette.broker.SessionRegistry;
import io.moquette.broker.SessionRegistry.EnqueuedMessage;
import io.moquette.broker.SessionRegistry.PublishedMessage;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.broker.unsafequeues.QueueException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentPersistentQueueTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPersistentQueueTest.class.getName());

    private static final int PAGE_SIZE = 5000;
    private static final int SEGMENT_SIZE = 1000;
    private static SegmentQueueRepository queueRepository;
    List<SessionMessageQueue<EnqueuedMessage>> queues = new ArrayList<>();

    private int queueIndex = 0;
    private static final String QUEUE_NAME = "test";

    @TempDir
    static Path tempQueueFolder;

    @BeforeAll
    public static void beforeAll() throws IOException, QueueException {
        System.setProperty("moquette.queue.debug", "false");
        queueRepository = new SegmentQueueRepository(tempQueueFolder.toFile().getAbsolutePath(), PAGE_SIZE, SEGMENT_SIZE);
    }

    @AfterEach
    public void tearDown() {
        for (SessionMessageQueue<EnqueuedMessage> queue : queues) {
            queue.closeAndPurge();
        }
        queues.clear();
    }

    @AfterAll
    public static void afterAll() {
        queueRepository.close();

    }

    private SessionMessageQueue<EnqueuedMessage> createQueue() {
        SessionMessageQueue<EnqueuedMessage> queue = queueRepository.getOrCreateQueue(QUEUE_NAME + (queueIndex++));
        queues.add(queue);
        return queue;
    }

    private static void createAndAddToQueue(SessionMessageQueue<EnqueuedMessage> queue, String topic, int totalSize) {
        final PublishedMessage msg1 = createMessage(topic, totalSize);
        msg1.retain();
        queue.enqueue(msg1);
        msg1.release();
    }

    private void createAndAddToQueues(String topic, int totalSize) {
        final PublishedMessage msg = createMessage(topic, totalSize);
        for (SessionMessageQueue<EnqueuedMessage> queue : queues) {
            msg.retain();
            queue.enqueue(msg);
        }
        msg.release();
    }

    private void assertAllEmpty(String message) {
        for (SessionMessageQueue<EnqueuedMessage> queue : queues) {
            assertTrue(queue.isEmpty(), message);
        }
    }

    private void assertAllNonEmpty(String message) {
        for (SessionMessageQueue<EnqueuedMessage> queue : queues) {
            assertFalse(queue.isEmpty(), message);
        }
    }

    private void dequeueFromAll(String expected) {
        for (SessionMessageQueue<EnqueuedMessage> queue : queues) {
            final SessionRegistry.PublishedMessage mesg = (PublishedMessage) queue.dequeue();
            assertEquals(expected, mesg.getTopic().toString());
        }
    }

    @Test
    public void testAdd() {
        LOGGER.info("testAdd");
        SessionMessageQueue<EnqueuedMessage> queue = queueRepository.getOrCreateQueue("testAdd");
        queues.add(queue);
        assertTrue(queue.isEmpty(), "Queue must start empty.");
        createAndAddToQueue(queue, "Hello", 100);
        assertFalse(queue.isEmpty(), "Queue must not be empty after adding.");
        createAndAddToQueue(queue, "world", 100);
        assertFalse(queue.isEmpty(), "Queue must not be empty after adding.");

        assertEquals("Hello", ((PublishedMessage) queue.dequeue()).getTopic().toString());
        assertEquals("world", ((PublishedMessage) queue.dequeue()).getTopic().toString());
        assertAllEmpty("After dequeueing all, queue must be empty");

        createAndAddToQueue(queue, "Hello", 100);
        assertFalse(queue.isEmpty(), "Queue must not be empty after adding.");
        assertEquals("Hello", ((PublishedMessage) queue.dequeue()).getTopic().toString());
        assertAllEmpty("After dequeueing all, queue must be empty");
    }

    @Test
    public void testAdd2() {
        LOGGER.info("testAdd2");
        testAddX(2);
    }

    @Test
    public void testAdd10() {
        LOGGER.info("testAdd10");
        testAddX(10);
    }

    public void testAddX(int x) {
        for (int i = 0; i < x; i++) {
            createQueue();
        }
        assertAllEmpty("Queue must start empty.");

        createAndAddToQueues("Hello", 100);
        assertAllNonEmpty("Queue must not be empty after adding.");

        createAndAddToQueues("world", 100);
        assertAllNonEmpty("Queue must not be empty after adding.");

        dequeueFromAll("Hello");
        assertAllNonEmpty("After dequeueing one, queue must not be empty");

        createAndAddToQueues("crazy", 100);
        assertAllNonEmpty("Queue must not be empty after adding.");

        dequeueFromAll("world");
        assertAllNonEmpty("Queue must not be empty after adding.");

        dequeueFromAll("crazy");
        assertAllEmpty("After dequeueing all, queue must be empty");
    }
    private static String body;

    private static String getBody(int bodySize) {
        if (body == null || body.length() != bodySize) {
            char a = 'A';
            char z = 'Z';
            char curChar = a;
            StringBuilder bodyString = new StringBuilder();
            for (int i = 0; i < bodySize; i++) {
                bodyString.append(curChar);
                if (curChar == z) {
                    curChar = a;
                } else {
                    curChar++;
                }
            }
            body = bodyString.toString();
        }
        return body;
    }

    private static void checkMessage(PublishedMessage message, String expTopic) {
        assertEquals(expTopic, message.getTopic().toString());
        final String receivedBody = message.getPayload().toString(UTF_8);
        final String expectedBody = getBody(receivedBody.length());
        assertEquals(expectedBody, receivedBody);
    }

    private static PublishedMessage createMessage(String topic, int totalMessageSize) {
        // 4 totalSize + 1 msgType + 1 qos + 4 topicSize + 4 bodySize = 14
        int publishedMessageSerializedHeaderSize =
            4 + // totalSize
            1 + // msgType
            1 + // qos
            4 + // topicSize
            4 + // bodySize
            + 4 + Instant.MAX.toString().getBytes(StandardCharsets.UTF_8).length; // message expiry
        int bodySize = totalMessageSize - publishedMessageSerializedHeaderSize - topic.getBytes(UTF_8).length;
        final ByteBuf payload = Unpooled.wrappedBuffer(getBody(bodySize).getBytes(StandardCharsets.UTF_8));
        return new PublishedMessage(Topic.asTopic(topic), MqttQoS.AT_LEAST_ONCE, payload, false, Instant.MAX);
    }

    @Test
    public void testPerformance() {
        LOGGER.info("testPerformance");
        SessionMessageQueue<EnqueuedMessage> queue = queueRepository.getOrCreateQueue("testPerformance");
        queues.add(queue);
        final String topic = "Hello";
        final int numIterations = 10_000;
        final int perIteration = 3;
        // With a total (in-queue) message size of 201, and a segment size of 1000
        // we can be sure we hit all corner cases.
        final int totalMessageSize = 201;
        int countPush = 0;
        int countPull = 0;
        int j = 0;
        final String message = "Queue should have contained " + perIteration + " items";
        try {
            for (int i = 0; i < numIterations; i++) {
                for (j = 0; j < perIteration; j++) {
                    countPush++;
                    LOGGER.debug("push {}, {}", countPush, j);
                    createAndAddToQueue(queue, topic, totalMessageSize);
                }
                j = 0;
                while (!queue.isEmpty()) {
                    countPull++;
                    j++;
                    LOGGER.debug("pull {}, {}", countPull, j);
                    final PublishedMessage msg = (PublishedMessage) queue.dequeue();
                    checkMessage(msg, topic);
                }
                assertEquals(perIteration, j, message);
            }
        } catch (Exception ex) {
            LOGGER.error("", ex);
            Assertions.fail("Failed on push count " + countPush + ", pull count " + countPull + ", j " + j, ex);
        }
        assertTrue(queue.isEmpty(), "should be empty");
    }

    @Test
    public void testReloadFromPersistedState() {
        LOGGER.info("testReloadFromPersistedState");
        SessionMessageQueue<EnqueuedMessage> queue = queueRepository.getOrCreateQueue("testReloadFromPersistedState");
        queues.add(queue);
        createAndAddToQueue(queue, "Hello", 100);
        createAndAddToQueue(queue, "crazy", 100);
        createAndAddToQueue(queue, "world", 100);
        assertEquals("Hello", ((PublishedMessage) queue.dequeue()).getTopic().toString());

        queue = queueRepository.getOrCreateQueue("testReloadFromPersistedState");

        assertEquals("crazy", ((PublishedMessage) queue.dequeue()).getTopic().toString());
        assertEquals("world", ((PublishedMessage) queue.dequeue()).getTopic().toString());
        assertTrue(queue.isEmpty(), "should be empty");
    }
}

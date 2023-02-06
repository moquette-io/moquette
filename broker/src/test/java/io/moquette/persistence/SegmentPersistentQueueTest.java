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
package io.moquette.persistence;

import io.moquette.broker.SessionMessageQueue;
import io.moquette.broker.SessionRegistry;
import io.moquette.broker.SessionRegistry.EnqueuedMessage;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.broker.unsafequeues.QueueException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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

    private static SegmentQueueRepository queueRepository;
    List<SessionMessageQueue<EnqueuedMessage>> queues = new ArrayList<>();

    private int queueIndex = 0;
    private static final String QUEUE_NAME = "test";

    @TempDir
    static Path tempQueueFolder;

    @BeforeAll
    public static void beforeAll() throws IOException, QueueException {
        queueRepository = new SegmentQueueRepository(tempQueueFolder.toFile().getAbsolutePath());
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

    private static void createAndAddToQueue(SessionMessageQueue<EnqueuedMessage> queue, String message) {
        final SessionRegistry.PublishedMessage msg1 = createMessage(message);
        msg1.retain();
        queue.enqueue(msg1);
        msg1.release();
    }

    private void createAndAddToQueues(String message) {
        final SessionRegistry.PublishedMessage msg1 = createMessage(message);
        for (SessionMessageQueue<EnqueuedMessage> queue : queues) {
            msg1.retain();
            queue.enqueue(msg1);
        }
        msg1.release();
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
            final SessionRegistry.PublishedMessage mesg = (SessionRegistry.PublishedMessage) queue.dequeue();
            assertEquals(expected, mesg.getTopic().toString());
        }
    }

    @Test
    public void testAdd() {
        LOGGER.info("testAdd");
        SessionMessageQueue<EnqueuedMessage> queue = queueRepository.getOrCreateQueue("testAdd");
        queues.add(queue);
        assertTrue(queue.isEmpty(), "Queue must start empty.");
        createAndAddToQueue(queue, "Hello");
        assertFalse(queue.isEmpty(), "Queue must not be empty after adding.");
        createAndAddToQueue(queue, "world");
        assertFalse(queue.isEmpty(), "Queue must not be empty after adding.");

        assertEquals("Hello", ((SessionRegistry.PublishedMessage) queue.dequeue()).getTopic().toString());
        assertEquals("world", ((SessionRegistry.PublishedMessage) queue.dequeue()).getTopic().toString());
        assertAllEmpty("After dequeueing all, queue must be empty");

        createAndAddToQueue(queue, "Hello");
        assertFalse(queue.isEmpty(), "Queue must not be empty after adding.");
        assertEquals("Hello", ((SessionRegistry.PublishedMessage) queue.dequeue()).getTopic().toString());
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

        createAndAddToQueues("Hello");
        assertAllNonEmpty("Queue must not be empty after adding.");

        createAndAddToQueues("world");
        assertAllNonEmpty("Queue must not be empty after adding.");

        dequeueFromAll("Hello");
        assertAllNonEmpty("After dequeueing one, queue must not be empty");

        createAndAddToQueues("crazy");
        assertAllNonEmpty("Queue must not be empty after adding.");

        dequeueFromAll("world");
        assertAllNonEmpty("Queue must not be empty after adding.");

        dequeueFromAll("crazy");
        assertAllEmpty("After dequeueing all, queue must be empty");
    }

    private static SessionRegistry.PublishedMessage createMessage(String name) {
        final ByteBuf payload = Unpooled.wrappedBuffer(name.getBytes(StandardCharsets.UTF_8));
        return new SessionRegistry.PublishedMessage(Topic.asTopic(name), MqttQoS.AT_LEAST_ONCE, payload, false);
    }

    @Test
    public void testPerformance() {
        LOGGER.info("testPerformance");
        SessionMessageQueue<EnqueuedMessage> queue = queueRepository.getOrCreateQueue("testPerformance");
        queues.add(queue);
        final int numIterations = 1_000_000;
        final int perIteration = 10;
        int count = 0;
        int j = 0;
        final String message = "Queue should have contained " + perIteration + " items";
        try {
            for (int i = 0; i < numIterations; i++) {
                for (j = 0; j < perIteration; j++) {
                    createAndAddToQueue(queue, "Hello");
                    count++;
                }
                j = 0;
                while (!queue.isEmpty()) {
                    assertEquals("Hello", ((SessionRegistry.PublishedMessage) queue.dequeue()).getTopic().toString());
                    j++;
                }
                assertEquals(perIteration, j, message);
            }
        } catch (Exception ex) {
            Assertions.fail("Failed on count " + count + ", j " + j, ex);
        }
        assertTrue(queue.isEmpty(), "should be empty");
    }

    @Test
    public void testReloadFromPersistedState() {
        LOGGER.info("testReloadFromPersistedState");
        SessionMessageQueue<EnqueuedMessage> queue = queueRepository.getOrCreateQueue("testReloadFromPersistedState");
        queues.add(queue);
        createAndAddToQueue(queue, "Hello");
        createAndAddToQueue(queue, "crazy");
        createAndAddToQueue(queue, "world");
        assertEquals("Hello", ((SessionRegistry.PublishedMessage) queue.dequeue()).getTopic().toString());

        queue = queueRepository.getOrCreateQueue("testReloadFromPersistedState");

        assertEquals("crazy", ((SessionRegistry.PublishedMessage) queue.dequeue()).getTopic().toString());
        assertEquals("world", ((SessionRegistry.PublishedMessage) queue.dequeue()).getTopic().toString());
        assertTrue(queue.isEmpty(), "should be empty");
    }
}

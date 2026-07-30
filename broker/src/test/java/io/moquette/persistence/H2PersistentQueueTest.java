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

import io.moquette.BrokerConstants;
import io.moquette.broker.SessionRegistry;
import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

public class H2PersistentQueueTest extends H2BaseTest {

    @Test
    public void testAdd() {
        H2PersistentQueue sut = new H2PersistentQueue(this.mvStore, "test");

        sut.enqueue(createMessage("Hello"));
        sut.enqueue(createMessage("world"));

        assertEquals("Hello", ((SessionRegistry.PublishedMessage) sut.dequeue()).getTopic().toString());
        assertEquals("world", ((SessionRegistry.PublishedMessage) sut.dequeue()).getTopic().toString());
        assertTrue(sut.isEmpty(), "dequeue effectively remove elements from queue");
    }

    private SessionRegistry.PublishedMessage createMessage(String name) {
        final ByteBuf payload = Unpooled.wrappedBuffer(name.getBytes(StandardCharsets.UTF_8));
        return new SessionRegistry.PublishedMessage(Topic.asTopic(name), MqttQoS.AT_LEAST_ONCE, payload, false, Instant.MAX);
    }

    @Test
    public void testPoll() {
        H2PersistentQueue sut = new H2PersistentQueue(this.mvStore, "test");
        sut.enqueue(createMessage("Hello"));
        sut.enqueue(createMessage("world"));

        assertEquals("Hello", ((SessionRegistry.PublishedMessage) sut.dequeue()).getTopic().toString());
        assertEquals("world", ((SessionRegistry.PublishedMessage) sut.dequeue()).getTopic().toString());
        assertTrue(sut.isEmpty(), "after poll 2 elements inserted before, should be empty");
    }

    @Disabled
    @Test
    public void testPerformance() {
        H2PersistentQueue sut = new H2PersistentQueue(this.mvStore, "test");

        int numIterations = 10000000;
        for (int i = 0; i < numIterations; i++) {
            sut.enqueue(createMessage("Hello"));
        }
        mvStore.commit();

        for (int i = 0; i < numIterations; i++) {
            assertEquals("Hello", ((SessionRegistry.PublishedMessage) sut.dequeue()).getTopic().toString());
        }

        assertTrue(sut.isEmpty(), "should be empty");
    }

    @Test
    public void testReloadFromPersistedState() {
        H2PersistentQueue before = new H2PersistentQueue(this.mvStore, "test");
        before.enqueue(createMessage("Hello"));
        before.enqueue(createMessage("crazy"));
        before.enqueue(createMessage("world"));
        assertEquals("Hello", ((SessionRegistry.PublishedMessage) before.dequeue()).getTopic().toString());
        this.mvStore.commit();
        this.mvStore.close();

        this.mvStore = new MVStore.Builder()
            .fileName(BrokerConstants.DEFAULT_PERSISTENT_PATH)
            .autoCommitDisabled()
            .open();

        //now reload the persisted state
        H2PersistentQueue after = new H2PersistentQueue(this.mvStore, "test");

        assertEquals("crazy", ((SessionRegistry.PublishedMessage) after.dequeue()).getTopic().toString());
        assertEquals("world", ((SessionRegistry.PublishedMessage) after.dequeue()).getTopic().toString());
        assertTrue(after.isEmpty(), "should be empty");
    }

    @Test
    public void givenAClientIdEndingInMetaWhenBothClientsUseDurableQueuesThenTheirMapsDoNotCollide() {
        // "sensor_meta"'s message map ("queue_sensor_meta") previously aliased "sensor"'s metadata map,
        // corrupting both durable queues. Each queue must now keep only its own messages.
        H2PersistentQueue victim = new H2PersistentQueue(this.mvStore, "sensor");
        H2PersistentQueue attacker = new H2PersistentQueue(this.mvStore, "sensor_meta");

        victim.enqueue(createMessage("victim-message"));
        attacker.enqueue(createMessage("attacker-message"));

        assertEquals("victim-message", ((SessionRegistry.PublishedMessage) victim.dequeue()).getTopic().toString());
        assertTrue(victim.isEmpty(), "victim queue must be unaffected by the other client");
        assertEquals("attacker-message", ((SessionRegistry.PublishedMessage) attacker.dequeue()).getTopic().toString());
        assertTrue(attacker.isEmpty(), "attacker queue must hold only its own message");
    }

    @Test
    public void givenALegacyMetadataMapWhenTheQueueIsOpenedThenHeadAndTailAreMigratedAndTheLegacyMapRemoved() {
        // Reproduce a pre-rename store: messages under "queue_test" and head/tail under "queue_test_meta".
        final MVMap.Builder<Long, SessionRegistry.EnqueuedMessage> messageTypeBuilder =
            new MVMap.Builder<Long, SessionRegistry.EnqueuedMessage>().valueType(new EnqueuedMessageValueType());
        MVMap<Long, SessionRegistry.EnqueuedMessage> legacyMessages = this.mvStore.openMap("queue_test", messageTypeBuilder);
        legacyMessages.put(0L, createMessage("Hello"));
        legacyMessages.put(1L, createMessage("crazy"));
        legacyMessages.put(2L, createMessage("world"));
        MVMap<String, Long> legacyMetadata = this.mvStore.openMap("queue_test_meta");
        legacyMetadata.put("head", 3L);
        legacyMetadata.put("tail", 1L);

        H2PersistentQueue sut = new H2PersistentQueue(this.mvStore, "test");

        // tail was migrated as 1, so the already-dequeued "Hello" is not replayed.
        assertEquals("crazy", ((SessionRegistry.PublishedMessage) sut.dequeue()).getTopic().toString());
        assertEquals("world", ((SessionRegistry.PublishedMessage) sut.dequeue()).getTopic().toString());
        assertTrue(sut.isEmpty(), "should be empty after draining the migrated queue");
        assertFalse(this.mvStore.hasMap("queue_test_meta"), "the legacy metadata map must be removed");
    }
}

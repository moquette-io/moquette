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
import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

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
        return new SessionRegistry.PublishedMessage(Topic.asTopic(name), MqttQoS.AT_LEAST_ONCE, payload, false);
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
}

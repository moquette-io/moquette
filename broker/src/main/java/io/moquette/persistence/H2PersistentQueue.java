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

import io.moquette.broker.AbstractSessionMessageQueue;
import io.moquette.broker.SessionMessageQueue;
import io.moquette.broker.SessionRegistry;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import java.util.concurrent.atomic.AtomicLong;

class H2PersistentQueue extends AbstractSessionMessageQueue<SessionRegistry.EnqueuedMessage> {

    // The message map and the metadata map must live in disjoint name spaces. The queue name is a raw
    // client id, so deriving both names from a shared "queue_" prefix let a client id ending in "_meta"
    // make its message map ("queue_" + "victim_meta") collide with another client's metadata map
    // ("queue_" + "victim" + "_meta") -> durable cross-session corruption. The metadata prefix now differs
    // from the message prefix at a fixed position, so no queue name can make the two names collide.
    private static final String MESSAGES_MAP_PREFIX = "queue_";
    private static final String METADATA_MAP_PREFIX = "queuemeta_";
    private static final String LEGACY_METADATA_MAP_SUFFIX = "_meta";

    private final MVMap<Long, SessionRegistry.EnqueuedMessage> queueMap;
    private final MVMap<String, Long> metadataMap;
    private final AtomicLong head;
    private final AtomicLong tail;
    private final MVStore store;
    private final String queueName;

    H2PersistentQueue(MVStore store, String queueName) {
        if (queueName == null || queueName.isEmpty()) {
            throw new IllegalArgumentException("queueName parameter can't be empty or null");
        }
        final MVMap.Builder<Long, SessionRegistry.EnqueuedMessage> messageTypeBuilder =
            new MVMap.Builder<Long, SessionRegistry.EnqueuedMessage>()
                .valueType(new EnqueuedMessageValueType());

        this.store = store;
        this.queueName = queueName;
        this.queueMap = this.store.openMap(MESSAGES_MAP_PREFIX + this.queueName, messageTypeBuilder);
        this.metadataMap = store.openMap(METADATA_MAP_PREFIX + queueName);
        migrateLegacyMetadata(store, queueName, this.metadataMap);

        //setup head index
        long headIdx = 0L;
        if (this.metadataMap.containsKey("head")) {
            headIdx = this.metadataMap.get("head");
        } else {
            this.metadataMap.put("head", headIdx);
        }
        this.head = new AtomicLong(headIdx);

        //setup tail index
        long tailIdx = 0L;
        if (this.metadataMap.containsKey("tail")) {
            tailIdx = this.metadataMap.get("tail");
        } else {
            this.metadataMap.put("tail", tailIdx);
        }
        this.tail = new AtomicLong(tailIdx);
    }

    @Override
    public void enqueue(SessionRegistry.EnqueuedMessage t) {
        checkEnqueuePreconditions(t);
        final long nextHead = head.getAndIncrement();
        this.queueMap.put(nextHead, t);
        this.metadataMap.put("head", nextHead + 1);
    }

    @Override
    public SessionRegistry.EnqueuedMessage dequeue() {
        checkDequeuePreconditions();
        if (head.equals(tail)) {
            return null;
        }
        final long nextTail = tail.getAndIncrement();
        final SessionRegistry.EnqueuedMessage tail = this.queueMap.get(nextTail);
        queueMap.remove(nextTail);
        this.metadataMap.put("tail", nextTail + 1);
        return tail;
    }

    @Override
    public boolean isEmpty() {
        checkIsEmptyPreconditions();
        return (this.head.intValue() - this.tail.intValue()) == 0;
    }

    @Override
    public void closeAndPurge() {
        this.closed = true;
        dropQueue(this.queueName);
    }

    private void dropQueue(String queueName) {
        store.removeMap(store.openMap(MESSAGES_MAP_PREFIX + queueName));
        store.removeMap(store.openMap(METADATA_MAP_PREFIX + queueName));
        final String legacyMetadata = MESSAGES_MAP_PREFIX + queueName + LEGACY_METADATA_MAP_SUFFIX;
        if (store.hasMap(legacyMetadata)) {
            store.removeMap(store.openMap(legacyMetadata));
        }
    }

    // Stores written before the rename kept the head/tail under MESSAGES_MAP_PREFIX + queueName + "_meta".
    // Move them into the new, collision-free map once and drop the legacy one, so a client id ending in
    // "_meta" can no longer reopen it as its own message map.
    private static void migrateLegacyMetadata(MVStore store, String queueName, MVMap<String, Long> metadataMap) {
        final String legacyName = MESSAGES_MAP_PREFIX + queueName + LEGACY_METADATA_MAP_SUFFIX;
        if (!metadataMap.isEmpty() || !store.hasMap(legacyName)) {
            return;
        }
        final MVMap<String, Long> legacy = store.openMap(legacyName);
        if (legacy.containsKey("head")) {
            metadataMap.put("head", legacy.get("head"));
        }
        if (legacy.containsKey("tail")) {
            metadataMap.put("tail", legacy.get("tail"));
        }
        store.removeMap(legacy);
    }

}

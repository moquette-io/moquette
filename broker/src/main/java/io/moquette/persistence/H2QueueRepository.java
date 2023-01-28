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

import io.moquette.broker.IQueueRepository;
import io.moquette.broker.SessionMessageQueue;
import io.moquette.broker.SessionRegistry.EnqueuedMessage;
import org.h2.mvstore.MVStore;

import java.util.Set;
import java.util.stream.Collectors;

public class H2QueueRepository implements IQueueRepository {

    private MVStore mvStore;

    public H2QueueRepository(MVStore mvStore) {
        this.mvStore = mvStore;
    }

    @Override
    public Set<String> listQueueNames() {
        return mvStore.getMapNames().stream()
            .filter(name -> name.startsWith("queue_") && !name.endsWith("_meta"))
            .map(name -> name.substring("queue_".length()))
            .collect(Collectors.toSet());
    }

    @Override
    public boolean containsQueue(String queueName) {
        return mvStore.hasMap("queue_" + queueName);
    }

    @Override
    public SessionMessageQueue<EnqueuedMessage> getOrCreateQueue(String clientId) {
        return new H2PersistentQueue(mvStore, clientId);
    }

    @Override
    public void close() {
        // No-op
    }
}

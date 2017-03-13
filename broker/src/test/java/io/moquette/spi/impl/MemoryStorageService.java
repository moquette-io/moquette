/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
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

package io.moquette.spi.impl;

import java.util.HashMap;
import java.util.Map;

import io.moquette.spi.IMessagesStore;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.MessageGUID;
import io.moquette.spi.persistence.IPersistentStore;
import io.moquette.spi.persistence.MemorySessionStore;

public class MemoryStorageService implements IPersistentStore {

    private MemorySessionStore m_sessionsStore;
    private MemoryMessagesStore m_messagesStore;

    public void initStore() {
        Map<String, Map<Integer, MessageGUID>> messageToGuids = new HashMap<>();
        m_messagesStore = new MemoryMessagesStore(messageToGuids);
        m_sessionsStore = new MemorySessionStore(m_messagesStore, messageToGuids);
    }

    public IMessagesStore messagesStore() {
        return m_messagesStore;
    }

    public ISessionsStore sessionsStore() {
        return m_sessionsStore;
    }

    @Override
    public void close() {
    }

}

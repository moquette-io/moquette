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

import io.moquette.spi.IMessagesStore;
import io.moquette.spi.IMatchingCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import static io.moquette.spi.impl.Utils.defaultGet;

/**
 * @author andrea
 */
public class MemoryMessagesStore implements IMessagesStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryMessagesStore.class);

    private Map<String, UUID> m_retainedStore = new HashMap<>();
    private Map<UUID, StoredMessage> m_persistentMessageStore = new HashMap<>();
    private Map<String, Map<Integer, UUID>> m_messageToGuids;

    MemoryMessagesStore(Map<String, Map<Integer, UUID>> messageToGuids) {
        m_messageToGuids = messageToGuids;
    }

    @Override
    public void initStore() {
    }

    @Override
    public void storeRetained(String topic, UUID guid) {
        m_retainedStore.put(topic, guid);
    }

    @Override
    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {
        LOG.debug("searchMatching scanning all retained messages, presents are {}", m_retainedStore.size());

        List<StoredMessage> results = new ArrayList<>();

        for (Map.Entry<String, UUID> entry : m_retainedStore.entrySet()) {
            final UUID guid = entry.getValue();
            StoredMessage storedMsg = m_persistentMessageStore.get(guid);
            if (condition.match(entry.getKey())) {
                results.add(storedMsg);
            }
        }

        return results;
    }

    @Override
    public UUID storePublishForFuture(StoredMessage storedMessage) {
        LOG.debug("storePublishForFuture store evt {}", storedMessage);
        UUID guid = UUID.randomUUID();
        storedMessage.setGuid(guid);
        m_persistentMessageStore.put(guid, storedMessage);
        HashMap<Integer, UUID> guids = (HashMap<Integer, UUID>) defaultGet(
                m_messageToGuids,
                storedMessage.getClientID(),
                new HashMap<Integer, UUID>());
        guids.put(storedMessage.getMessageID(), guid);
        return guid;
    }

    @Override
    public void dropMessagesInSession(String clientID) {
        Map<Integer, UUID> UUIDMap = m_messageToGuids.get(clientID);
        if (UUIDMap == null || UUIDMap.isEmpty()) {
            return;
        }
        for (UUID guid : UUIDMap.values()) {
            m_persistentMessageStore.remove(guid);
        }
    }

    @Override
    public StoredMessage getMessageByGuid(UUID guid) {
        return m_persistentMessageStore.get(guid);
    }

    @Override
    public void cleanRetained(String topic) {
        m_retainedStore.remove(topic);
    }

    @Override
    public int getPendingPublishMessages(String clientID) {
        Map<Integer, UUID> messageToGuids = m_messageToGuids.get(clientID);
        if (messageToGuids == null)
            return 0;
        else
            return messageToGuids.size();
    }
}

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
import io.moquette.spi.MessageGUID;
import io.moquette.spi.impl.subscriptions.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import static io.moquette.spi.impl.Utils.defaultGet;

/**
 * @author andrea
 */
public class MemoryMessagesStore implements IMessagesStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryMessagesStore.class);

    private Map<Topic, MessageGUID> m_retainedStore = new HashMap<>();
    private Map<MessageGUID, StoredMessage> m_persistentMessageStore = new HashMap<>();
    private Map<String, Map<Integer, MessageGUID>> m_messageToGuids;

    MemoryMessagesStore(Map<String, Map<Integer, MessageGUID>> messageToGuids) {
        m_messageToGuids = messageToGuids;
    }

    @Override
    public void initStore() {
    }

    @Override
    public void storeRetained(Topic topic, MessageGUID guid) {
        m_retainedStore.put(topic, guid);
    }

    @Override
    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {
        LOG.debug("searchMatching scanning all retained messages, presents are {}", m_retainedStore.size());

        List<StoredMessage> results = new ArrayList<>();

        for (Map.Entry<Topic, MessageGUID> entry : m_retainedStore.entrySet()) {
            final MessageGUID guid = entry.getValue();
            StoredMessage storedMsg = m_persistentMessageStore.get(guid);
            if (condition.match(entry.getKey())) {
                results.add(storedMsg);
            }
        }

        return results;
    }

    @Override
    public MessageGUID storePublishForFuture(StoredMessage storedMessage) {
        LOG.debug("storePublishForFuture store evt {}", storedMessage);
        MessageGUID guid = new MessageGUID(UUID.randomUUID().toString());
        storedMessage.setGuid(guid);
        m_persistentMessageStore.put(guid, storedMessage);
        HashMap<Integer, MessageGUID> guids = (HashMap<Integer, MessageGUID>) defaultGet(
                m_messageToGuids,
                storedMessage.getClientID(),
                new HashMap<Integer, MessageGUID>());
        guids.put(storedMessage.getMessageID(), guid);
        return guid;
    }

    @Override
    public void dropMessagesInSession(String clientID) {
        Map<Integer, MessageGUID> messageGUIDMap = m_messageToGuids.get(clientID);
        if (messageGUIDMap == null || messageGUIDMap.isEmpty()) {
            return;
        }
        for (MessageGUID guid : messageGUIDMap.values()) {
            m_persistentMessageStore.remove(guid);
        }
    }

    @Override
    public StoredMessage getMessageByGuid(MessageGUID guid) {
        return m_persistentMessageStore.get(guid);
    }

    @Override
    public void cleanRetained(Topic topic) {
        m_retainedStore.remove(topic);
    }

    @Override
    public int getPendingPublishMessages(String clientID) {
        Map<Integer, MessageGUID> messageToGuids = m_messageToGuids.get(clientID);
        if (messageToGuids == null)
            return 0;
        else
            return messageToGuids.size();
    }

    @Override
    public MessageGUID mapToGuid(String clientID, int messageID) {
        HashMap<Integer, MessageGUID> guids = (HashMap<Integer, MessageGUID>) Utils
                .defaultGet(m_messageToGuids, clientID, new HashMap<Integer, MessageGUID>());
        return guids.get(messageID);
    }
}

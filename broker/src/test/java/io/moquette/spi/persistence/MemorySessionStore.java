/*
 * Copyright (c) 2012-2015 The original author or authors
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
package io.moquette.spi.persistence;

import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.MessageGUID;
import io.moquette.spi.impl.Utils;
import io.moquette.spi.impl.subscriptions.Subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 * @author andrea
 */
public class MemorySessionStore implements ISessionsStore {
    private static final Logger LOG = LoggerFactory.getLogger(MemorySessionStore.class);

    private Map<String, Set<Subscription>> m_persistentSubscriptions = new HashMap<>();

    private Map<String, MapDBPersistentStore.PersistentSession> m_persistentSessions = new HashMap<>();

    //maps clientID->[MessageId -> guid]
    private Map<String, Map<Integer, MessageGUID>> m_inflightStore = new HashMap<>();
//    private Map<String, Set<Integer>> m_inflightIDs = new HashMap<>();
    //maps clientID->[guid*]
    private Map<String, Set<MessageGUID>> m_enqueuedStore = new HashMap<>();
    //maps clientID->[MessageId -> guid]
    private Map<String, Map<Integer, MessageGUID>> m_secondPhaseStore = new HashMap<>();

    private Map<String, Map<Integer, MessageGUID>> m_messageToGuids;
    private final IMessagesStore m_messagesStore;

    public MemorySessionStore(IMessagesStore messagesStore, Map<String, Map<Integer, MessageGUID>> messageToGuids) {
        m_messageToGuids = messageToGuids;
        this.m_messagesStore = messagesStore;
    }

    @Override
    public void removeSubscription(String topic, String clientID) {
        LOG.debug("removeSubscription topic filter: {} for clientID: {}", topic, clientID);
        if (!m_persistentSubscriptions.containsKey(clientID)) {
            return;
        }
        Set<Subscription> clientSubscriptions = m_persistentSubscriptions.get(clientID);
        //search for the subscription to remove
        Subscription toBeRemoved = null;
        for (Subscription sub : clientSubscriptions) {
            if (sub.getTopicFilter().equals(topic)) {
                toBeRemoved = sub;
                break;
            }
        }

        if (toBeRemoved != null) {
            clientSubscriptions.remove(toBeRemoved);
        }
    }

    @Override
    public void initStore() {

    }

    @Override
    public void addNewSubscription(Subscription newSubscription) {
        final String clientID = newSubscription.getClientId();
        if (!m_persistentSubscriptions.containsKey(clientID)) {
            m_persistentSubscriptions.put(clientID, new HashSet<Subscription>());
        }

        Set<Subscription> subs = m_persistentSubscriptions.get(clientID);
        subs.remove(newSubscription); //same topic and clientID
        subs.add(newSubscription);
        m_persistentSubscriptions.put(clientID, subs);
    }

    @Override
    public void wipeSubscriptions(String clientID) {
        m_persistentSubscriptions.remove(clientID);
    }

    @Override
    public boolean contains(String clientID) {
        return m_persistentSubscriptions.containsKey(clientID);
    }

    @Override
    public ClientSession createNewSession(String clientID, boolean cleanSession) {
        LOG.debug("createNewSession for client <{}>", clientID);
        if (m_persistentSessions.containsKey(clientID)) {
            LOG.error("already exists a session for client <{}>, bad condition", clientID);
            throw new IllegalArgumentException("Can't create a session with the ID of an already existing" + clientID);
        }
        LOG.debug("clientID {} is a newcome, creating it's empty subscriptions set", clientID);
        m_persistentSubscriptions.put(clientID, new HashSet<Subscription>());
        m_persistentSessions.put(clientID, new MapDBPersistentStore.PersistentSession(cleanSession));
        return new ClientSession(clientID, m_messagesStore, this, cleanSession);
    }

    @Override
    public ClientSession sessionForClient(String clientID) {
        if (!m_persistentSessions.containsKey(clientID)) {
            return null;
        }

        MapDBPersistentStore.PersistentSession storedSession = m_persistentSessions.get(clientID);
        return new ClientSession(clientID, m_messagesStore, this, storedSession.cleanSession);
    }

    @Override
    public void updateCleanStatus(String clientID, boolean cleanSession) {
        m_persistentSessions.put(clientID, new MapDBPersistentStore.PersistentSession(cleanSession));
    }

    @Override
    public List<ClientTopicCouple> listAllSubscriptions() {
        List<ClientTopicCouple> allSubscriptions = new ArrayList<>();
        for (Map.Entry<String, Set<Subscription>> entry : m_persistentSubscriptions.entrySet()) {
            for (Subscription sub : entry.getValue()) {
                allSubscriptions.add(sub.asClientTopicCouple());
            }
        }
        return allSubscriptions;
    }

    @Override
    public Subscription getSubscription(ClientTopicCouple couple) {
        Set<Subscription> subscriptions = m_persistentSubscriptions.get(couple.clientID);
        if (subscriptions == null || subscriptions.isEmpty()) {
            return null;
        }
        for (Subscription sub : subscriptions) {
            if (sub.getTopicFilter().equals(couple.topicFilter)) {
                return sub;
            }
        }
        return null;
    }

    @Override
    public List<Subscription> getSubscriptions() {
        List<Subscription> subscriptions = new ArrayList<>();
        for (Map.Entry<String, Set<Subscription>> entry : m_persistentSubscriptions.entrySet()) {
            subscriptions.addAll(entry.getValue());
        }
        return subscriptions;
    }

    @Override
    public void inFlightAck(String clientID, int messageID) {
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            LOG.error("Can't find the inFlight record for client <{}>", clientID);
            return;
        }
        m.remove(messageID);
    }

    @Override
    public void inFlight(String clientID, int messageID, MessageGUID guid) {
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            m = new HashMap<>();
        }
        m.put(messageID, guid);
        this.m_inflightStore.put(clientID, m);
    }

    /**
     * Return the next valid packetIdentifier for the given client session.
     * */
    @Override
    public int nextPacketID(String clientID) {
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            m = new HashMap<>();
            int nextPacketId = 1;
            m.put(nextPacketId, null);
            return nextPacketId;
        }
        int maxId = Collections.max(m.keySet());
        int nextPacketId = (maxId + 1) % 0xFFFF;
        m.put(nextPacketId, null);
        return nextPacketId;
    }

    @Override
    public void bindToDeliver(MessageGUID guid, String clientID) {
        Set<MessageGUID> guids = Utils.defaultGet(m_enqueuedStore, clientID, new HashSet<MessageGUID>());
        guids.add(guid);
        m_enqueuedStore.put(clientID, guids);
    }

    @Override
    public Collection<MessageGUID> enqueued(String clientID) {
        return Utils.defaultGet(m_enqueuedStore, clientID, new HashSet<MessageGUID>());
    }

    @Override
    public void removeEnqueued(String clientID, MessageGUID guid) {
        Set<MessageGUID> guids = Utils.defaultGet(m_enqueuedStore, clientID, new HashSet<MessageGUID>());
        guids.remove(guid);
        m_enqueuedStore.put(clientID, guids);
    }

    @Override
    public void moveInFlightToSecondPhaseAckWaiting(String clientID, int messageID) {
        LOG.info("acknowledging inflight clientID <{}> messageID {}", clientID, messageID);
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            LOG.error("Can't find the inFlight record for client <{}>", clientID);
            return;
        }
        MessageGUID guid = m.remove(messageID);

        LOG.info("Moving to second phase store");
        Map<Integer, MessageGUID> messageIDs = Utils.defaultGet(m_secondPhaseStore, clientID, new HashMap<Integer, MessageGUID>());
        messageIDs.put(messageID, guid);
        m_secondPhaseStore.put(clientID, messageIDs);
    }

    @Override
    public MessageGUID secondPhaseAcknowledged(String clientID, int messageID) {
        Map<Integer, MessageGUID> messageIDs = Utils.defaultGet(m_secondPhaseStore, clientID, new HashMap<Integer, MessageGUID>());
        MessageGUID guid = messageIDs.remove(messageID);
        m_secondPhaseStore.put(clientID, messageIDs);
        return guid;
    }

    @Override
    public MessageGUID mapToGuid(String clientID, int messageID) {
        HashMap<Integer, MessageGUID> guids = (HashMap<Integer, MessageGUID>) Utils.defaultGet(m_messageToGuids,
                clientID, new HashMap<Integer, MessageGUID>());
        return guids.get(messageID);
    }

	@Override
	public StoredMessage getInflightMessage( String clientID, int messageID ) {
		return null;
	}
}

/*
 * Copyright (c) 2012-2017 The original author or authors
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
import io.moquette.server.Constants;
import io.moquette.server.config.IConfig;
import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.ISubscriptionsStore;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MemorySessionStore implements ISessionsStore, ISubscriptionsStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemorySessionStore.class);

    class Session {
        final String clientID;
        final ClientSession clientSession;
        final Map<Topic, Subscription> subscriptions = new ConcurrentHashMap<>();
        final AtomicReference<PersistentSession> persistentSession = new AtomicReference<>(null);
        final BlockingQueue<StoredMessage> queue = new ArrayBlockingQueue<>(Constants.MAX_MESSAGE_QUEUE);
        final Map<Integer, StoredMessage> secondPhaseStore = new ConcurrentHashMap<>();
        final Map<Integer, StoredMessage> outboundFlightMessages =
                Collections.synchronizedMap(new HashMap<Integer, StoredMessage>());
        final Map<Integer, StoredMessage> inboundFlightMessages = new ConcurrentHashMap<>();

        Session(String clientID, ClientSession clientSession) {
            this.clientID = clientID;
            this.clientSession = clientSession;
        }

        public void clean() {
            LOG.info("Cleaning Session. ClientId={}", clientID);

            secondPhaseStore.clear();
            outboundFlightMessages.clear();
            inboundFlightMessages.clear();
            subscriptions.clear();
            queue.clear();
        }
    }

    private Cache<String, Session> sessions;

    public MemorySessionStore(IConfig config) {
        int expireInDays = Integer.parseInt(config.getProperty(BrokerConstants.STORAGE_CLASS_SESSION_EXPIRE, "28"));
        int maxSessions = Integer.parseInt(config.getProperty(BrokerConstants.STORAGE_CLASS_MAX_SESSIONS, "5000"));

        sessions = CacheBuilder
                .newBuilder()
                .expireAfterAccess(expireInDays, TimeUnit.DAYS)
                .removalListener(k -> {
                    LOG.error("Session for CID: {} removed Cause: {}", k.getKey(), k.getCause());
                    ((Session) k.getValue()).clean();
                })
                .maximumSize(maxSessions)
                .build();

    }

    private Session getSession(String clientID) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            throw new RuntimeException("Can't find the session for client <" + clientID + ">");
        }
        return session;
    }

    @Override
    public void removeSubscription(Topic topic, String clientID) {
        LOG.debug("removeSubscription topic filter: {} for clientID: {}", topic, clientID);
        getSession(clientID).subscriptions.remove(topic);
    }

    @Override
    public void initStore() {
    }

    @Override
    public ISubscriptionsStore subscriptionStore() {
        return this;
    }

    @Override
    public void addNewSubscription(Subscription newSubscription) {
        final String clientID = newSubscription.getClientId();
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return;
        }

        session.subscriptions.put(newSubscription.getTopicFilter(), newSubscription);
    }

    @Override
    public void wipeSubscriptions(String clientID) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return;
        }

        session.subscriptions.clear();
    }

    @Override
    public boolean contains(String clientID) {
        return sessions.getIfPresent(clientID) != null;
    }

    @Override
    public ClientSession createNewSession(String clientID, boolean cleanSession) {
        LOG.debug("createNewSession for client <{}>", clientID);
        Session session = sessions.getIfPresent(clientID);
        if (session != null) {
            LOG.error("already exists a session for client <{}>, bad condition", clientID);
            throw new IllegalArgumentException("Can't create a session with the ID of an already existing" + clientID);
        }
        LOG.debug("clientID {} is a newcome, creating it's empty subscriptions set", clientID);
        session = new Session(clientID, new ClientSession(clientID, this, this, cleanSession));
        session.persistentSession.set(new PersistentSession(cleanSession));
        sessions.put(clientID, session);
        return session.clientSession;
    }

    @Override
    public ClientSession sessionForClient(String clientID) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return null;
        }

        PersistentSession storedSession = session.persistentSession.get();
        return new ClientSession(clientID, this, this, storedSession.cleanSession);
    }

    @Override
    public Collection<ClientSession> getAllSessions() {
        Collection<ClientSession> result = new ArrayList<>();
        for (Session entry : sessions.asMap().values()) {
            result.add(new ClientSession(entry.clientID, this, this, entry.persistentSession.get().cleanSession));
        }
        return result;
    }

    @Override
    public void updateCleanStatus(String clientID, boolean cleanSession) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return;
        }

        session.persistentSession.set(new PersistentSession(cleanSession));
    }

    @Override
    public List<ClientTopicCouple> listAllSubscriptions() {
        List<ClientTopicCouple> allSubscriptions = new ArrayList<>();
        for (Session entry : sessions.asMap().values()) {
            for (Subscription sub : entry.subscriptions.values()) {
                allSubscriptions.add(sub.asClientTopicCouple());
            }
        }
        return allSubscriptions;
    }

    @Override
    public Subscription getSubscription(ClientTopicCouple couple) {
        String clientID = couple.clientID;
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return null;
        }

        Map<Topic, Subscription> subscriptions = session.subscriptions;
        if (subscriptions == null || subscriptions.isEmpty()) {
            return null;
        }
        return subscriptions.get(couple.topicFilter);
    }

    @Override
    public List<Subscription> getSubscriptions() {
        List<Subscription> subscriptions = new ArrayList<>();
        for (Session entry : sessions.asMap().values()) {
            subscriptions.addAll(entry.subscriptions.values());
        }
        return subscriptions;
    }

    @Override
    public StoredMessage inFlightAck(String clientID, int messageID) {
        return getSession(clientID).outboundFlightMessages.remove(messageID);
    }

    @Override
    public void inFlight(String clientID, int messageID, StoredMessage msg) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return;
        }

        session.outboundFlightMessages.put(messageID, msg);
    }

    /**
     * Return the next valid packetIdentifier for the given client session.
     */
    @Override
    public int nextPacketID(String clientID) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return -1;
        }

        Map<Integer, StoredMessage> m = session.outboundFlightMessages;
        int maxId = m.keySet().isEmpty() ? 0 : Collections.max(m.keySet());
        int nextPacketId = (maxId + 1) % 0xFFFF;
        m.put(nextPacketId, null);
        return nextPacketId;
    }

    @Override
    public BlockingQueue<StoredMessage> queue(String clientID) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return null;
        }

        return session.queue;
    }

    @Override
    public void dropQueue(String clientID) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return;
        }

        session.queue.clear();
    }

    @Override
    public void moveInFlightToSecondPhaseAckWaiting(String clientID, int messageID, StoredMessage msg) {
        LOG.info("Moving msg inflight second phase store, clientID <{}> messageID {}", clientID, messageID);
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return;
        }

        session.secondPhaseStore.put(messageID, msg);
        session.outboundFlightMessages.put(messageID, msg);
    }

    @Override
    public StoredMessage secondPhaseAcknowledged(String clientID, int messageID) {
        LOG.info("Acknowledged message in second phase, clientID <{}> messageID {}", clientID, messageID);
        return getSession(clientID).secondPhaseStore.remove(messageID);
    }

    @Override
    public int getInflightMessagesNo(String clientID) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return 0;
        }

        return session.inboundFlightMessages.size() + session.secondPhaseStore.size()
            + session.outboundFlightMessages.size();
    }

    @Override
    public StoredMessage inboundInflight(String clientID, int messageID) {
        return getSession(clientID).inboundFlightMessages.get(messageID);
    }

    @Override
    public void markAsInboundInflight(String clientID, int messageID, StoredMessage msg) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return;
        }
        session.inboundFlightMessages.put(messageID, msg);
    }

    @Override
    public int getPendingPublishMessagesNo(String clientID) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return 0;
        }

        return session.queue.size();
    }

    @Override
    public int getSecondPhaseAckPendingMessages(String clientID) {
        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            return 0;
        }

        return session.secondPhaseStore.size();
    }

    @Override
    public void cleanSession(String clientID) {
        LOG.debug("Session cleanup for client <{}>", clientID);

        Session session = sessions.getIfPresent(clientID);
        if (session == null) {
            LOG.error("Can't find the session for client <{}>", clientID);
            return;
        }

        session.clean();
    }

    @Override
    public void heartBeat(String clientID) {
        sessions.getIfPresent(clientID);
    }
}

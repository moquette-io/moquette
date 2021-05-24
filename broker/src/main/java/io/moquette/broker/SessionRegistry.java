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
package io.moquette.broker;

import io.moquette.broker.Session.SessionStatus;
import io.moquette.broker.subscriptions.ISubscriptionsDirectory;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class SessionRegistry {

    public abstract static class EnqueuedMessage {

        /**
         * Releases any held resources. Must be called when the EnqueuedMessage is no
         * longer needed.
         */
        public void release() {}

        /**
         * Retains any held resources. Must be called when the EnqueuedMessage is added
         * to a store.
         */
        public void retain() {}
    }

    public static class PublishedMessage extends EnqueuedMessage {

        final Topic topic;
        final MqttQoS publishingQos;
        final ByteBuf payload;

        public PublishedMessage(Topic topic, MqttQoS publishingQos, ByteBuf payload) {
            this.topic = topic;
            this.publishingQos = publishingQos;
            this.payload = payload;
        }

        public Topic getTopic() {
            return topic;
        }

        public MqttQoS getPublishingQos() {
            return publishingQos;
        }

        public ByteBuf getPayload() {
            return payload;
        }

        @Override
        public void release() {
            payload.release();
        }

        @Override
        public void retain() {
            payload.retain();
        }

    }

    public static final class PubRelMarker extends EnqueuedMessage {
    }

    public enum CreationModeEnum {
        CREATED_CLEAN_NEW, REOPEN_EXISTING, DROP_EXISTING
    }

    public static class SessionCreationResult {

        final Session session;
        final CreationModeEnum mode;
        final boolean alreadyStored;

        public SessionCreationResult(Session session, CreationModeEnum mode, boolean alreadyStored) {
            this.session = session;
            this.mode = mode;
            this.alreadyStored = alreadyStored;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(SessionRegistry.class);

    private final ConcurrentMap<String, Session> pool = new ConcurrentHashMap<>();
    private final ISubscriptionsDirectory subscriptionsDirectory;
    private final IQueueRepository queueRepository;
    private final Authorizator authorizator;
    private final ConcurrentMap<String, Queue<SessionRegistry.EnqueuedMessage>> queues = new ConcurrentHashMap<>();

    SessionRegistry(ISubscriptionsDirectory subscriptionsDirectory,
                    IQueueRepository queueRepository,
                    Authorizator authorizator) {
        this.subscriptionsDirectory = subscriptionsDirectory;
        this.queueRepository = queueRepository;
        this.authorizator = authorizator;
        reloadPersistentQueues();
        recreateSessionPool();
    }

    private void reloadPersistentQueues() {
        final Map<String, Queue<EnqueuedMessage>> persistentQueues = queueRepository.listAllQueues();
        persistentQueues.forEach(queues::put);
    }

    private void recreateSessionPool() {
        for (String clientId : subscriptionsDirectory.listAllSessionIds()) {
            // if the subscriptions are present is obviously false
            final Queue<EnqueuedMessage> persistentQueue = queues.get(clientId);
            if (persistentQueue != null) {
                Session rehydrated = new Session(clientId, false, persistentQueue);
                pool.put(clientId, rehydrated);
            }
        }
    }

    SessionCreationResult createOrReopenSession(MqttConnectMessage msg, String clientId, String username) {
        SessionCreationResult postConnectAction;
        final Session newSession = createNewSession(msg, clientId);
        final Session oldSession = pool.get(clientId);
        if (oldSession == null) {
            // case 1
            postConnectAction = new SessionCreationResult(newSession, CreationModeEnum.CREATED_CLEAN_NEW, false);

            // publish the session
            final Session previous = pool.putIfAbsent(clientId, newSession);
            final boolean success = previous == null;

            if (success) {
                LOG.trace("case 1, not existing session with CId {}", clientId);
            } else {
                postConnectAction = reopenExistingSession(msg, clientId, previous, newSession, username);
            }
        } else {
            postConnectAction = reopenExistingSession(msg, clientId, oldSession, newSession, username);
        }
        return postConnectAction;
    }

    private SessionCreationResult reopenExistingSession(MqttConnectMessage msg, String clientId,
                                                        Session oldSession, Session newSession, String username) {
        final boolean newIsClean = msg.variableHeader().isCleanSession();
        final SessionCreationResult creationResult;
        if (oldSession.disconnected()) {
            if (newIsClean) {
                boolean result = oldSession.assignState(SessionStatus.DISCONNECTED, SessionStatus.CONNECTING);
                if (!result) {
                    throw new SessionCorruptedException("old session was already changed state");
                }

                // case 2
                // publish new session
                dropQueuesForClient(clientId);
                unsubscribe(oldSession);
                copySessionConfig(msg, oldSession);

                LOG.trace("case 2, oldSession with same CId {} disconnected", clientId);
                creationResult = new SessionCreationResult(oldSession, CreationModeEnum.CREATED_CLEAN_NEW, true);
            } else {
                final boolean connecting = oldSession.assignState(SessionStatus.DISCONNECTED, SessionStatus.CONNECTING);
                if (!connecting) {
                    throw new SessionCorruptedException("old session moved in connected state by other thread");
                }
                // case 3
                reactivateSubscriptions(oldSession, username);

                LOG.trace("case 3, oldSession with same CId {} disconnected", clientId);
                creationResult = new SessionCreationResult(oldSession, CreationModeEnum.REOPEN_EXISTING, true);
            }
        } else {
            // case 4
            LOG.trace("case 4, oldSession with same CId {} still connected, force to close", clientId);
            oldSession.closeImmediately();
            //remove(clientId);
            creationResult = new SessionCreationResult(newSession, CreationModeEnum.DROP_EXISTING, true);
        }

        if (creationResult.mode == CreationModeEnum.DROP_EXISTING) {
            LOG.debug("Drop session of already connected client with same id");
            if (!pool.replace(clientId, oldSession, newSession)) {
                //the other client was disconnecting and removed it's own session
                pool.put(clientId, newSession);
            }
        } else {
            LOG.debug("Replace session of client with same id");
            if (!pool.replace(clientId, oldSession, oldSession)) {
                throw new SessionCorruptedException("old session was already removed");
            }
        }

        // case not covered new session is clean true/false and old session not in CONNECTED/DISCONNECTED
        return creationResult;
    }

    private void reactivateSubscriptions(Session session, String username) {
        //verify if subscription still satisfy read ACL permissions
        for (Subscription existingSub : session.getSubscriptions()) {
            final boolean topicReadable = authorizator.canRead(existingSub.getTopicFilter(), username,
                                                               session.getClientID());
            if (!topicReadable) {
                subscriptionsDirectory.removeSubscription(existingSub.getTopicFilter(), session.getClientID());
            }
            // TODO
//            subscriptionsDirectory.reactivate(existingSub.getTopicFilter(), session.getClientID());
        }
    }

    private void unsubscribe(Session session) {
        for (Subscription existingSub : session.getSubscriptions()) {
            subscriptionsDirectory.removeSubscription(existingSub.getTopicFilter(), session.getClientID());
        }
    }

    private Session createNewSession(MqttConnectMessage msg, String clientId) {
        final boolean clean = msg.variableHeader().isCleanSession();
        final Queue<SessionRegistry.EnqueuedMessage> sessionQueue =
                    queues.computeIfAbsent(clientId, (String cli) -> queueRepository.createQueue(cli, clean));
        final Session newSession;
        if (msg.variableHeader().isWillFlag()) {
            final Session.Will will = createWill(msg);
            newSession = new Session(clientId, clean, will, sessionQueue);
        } else {
            newSession = new Session(clientId, clean, sessionQueue);
        }

        newSession.markConnecting();
        return newSession;
    }

    private void copySessionConfig(MqttConnectMessage msg, Session session) {
        final boolean clean = msg.variableHeader().isCleanSession();
        final Session.Will will;
        if (msg.variableHeader().isWillFlag()) {
            will = createWill(msg);
        } else {
            will = null;
        }
        session.update(clean, will);
    }

    private Session.Will createWill(MqttConnectMessage msg) {
        final ByteBuf willPayload = Unpooled.copiedBuffer(msg.payload().willMessageInBytes());
        final String willTopic = msg.payload().willTopic();
        final boolean retained = msg.variableHeader().isWillRetain();
        final MqttQoS qos = MqttQoS.valueOf(msg.variableHeader().willQos());
        return new Session.Will(willTopic, willPayload, qos, retained);
    }

    Session retrieve(String clientID) {
        return pool.get(clientID);
    }

    public void remove(Session session) {
        pool.remove(session.getClientID(), session);
    }

    private void dropQueuesForClient(String clientId) {
        queues.remove(clientId);
    }

    Collection<ClientDescriptor> listConnectedClients() {
        return pool.values().stream()
            .filter(Session::connected)
            .map(this::createClientDescriptor)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

    private Optional<ClientDescriptor> createClientDescriptor(Session s) {
        final String clientID = s.getClientID();
        final Optional<InetSocketAddress> remoteAddressOpt = s.remoteAddress();
        return remoteAddressOpt.map(r -> new ClientDescriptor(clientID, r.getHostString(), r.getPort()));
    }
}

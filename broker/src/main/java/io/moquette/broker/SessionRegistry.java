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
        CREATED_CLEAN_NEW, REOPEN_EXISTING
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

    SessionRegistry(ISubscriptionsDirectory subscriptionsDirectory,
                    IQueueRepository queueRepository,
                    Authorizator authorizator) {
        this.subscriptionsDirectory = subscriptionsDirectory;
        this.queueRepository = queueRepository;
        this.authorizator = authorizator;
        recreateSessionPool();
    }

    private void recreateSessionPool() {
        final Map<String, Queue<EnqueuedMessage>> persistentQueues = queueRepository.listAllQueues();
        for (String clientId : subscriptionsDirectory.listAllSessionIds()) {
            // if the subscriptions are present is obviously false
            final Queue<EnqueuedMessage> persistentQueue = persistentQueues.get(clientId);
            if (persistentQueue != null) {
                Session rehydrated = new Session(clientId, false, persistentQueue);
                pool.put(clientId, rehydrated);
            }
        }
    }

    SessionCreationResult openAndBindSession(MqttConnectMessage msg, String clientId, String username,
                                             MQTTConnection mqttConnection) {
        SessionCreationResult postConnectAction;
        final boolean newIsClean = msg.variableHeader().isCleanSession();
        final Session oldSession = pool.get(clientId);

        // A new session should be created in the following 3 cases:
        //  1. clean session = true for the new session
        //  2. No old session for the given client ID exists
        //  3. An old session for the client ID does exist, but it itself is a clean session
        if (newIsClean || oldSession == null || oldSession.isClean()) {
            final Session newSession = createNewSession(msg, clientId);

            postConnectAction = new SessionCreationResult(newSession, CreationModeEnum.CREATED_CLEAN_NEW, false);

            // Bind MQTT connection to new Session and store it in the session pool. This is needed to prevent
            // dangling sessions. If this session is terminated prior to it being associated with the mqttConnection,
            // then the client will never be disconnected, yet they will not be able to receive any messages
            LOG.trace("case 1, creating new session with CId {}, clean session {}", clientId, newIsClean);
            newSession.bind(mqttConnection);
            final Session previous = pool.put(clientId, newSession);

            // Disconnect the previously connected client
            if (previous != null) {
                LOG.trace("terminating previous client session with CId {}", clientId);
                terminateSession(previous);
            }
        } else {
            postConnectAction = reopenExistingSession(clientId, oldSession, username, mqttConnection);
        }

        return postConnectAction;
    }

    private SessionCreationResult reopenExistingSession(String clientId, Session oldSession, String username,
                                                        MQTTConnection mqttConnection) {
        // Applying De Morgan's law:
        // !newIsClean && oldSession != null && !oldSession.isClean()
        if (oldSession.disconnected()) {
            final boolean connecting = oldSession.markConnecting();
            if (!connecting) {
                throw new SessionCorruptedException("old session moved in connected state by other thread");
            }
            // case 2
            LOG.trace("case 2, oldSession with same CId {} disconnected", clientId);
            oldSession.bind(mqttConnection);
            reactivateSubscriptions(oldSession, username);
        } else {
            // case 3
            LOG.trace("case 3, oldSession with same CId {} still connected, force to close", clientId);
            final boolean didReplace = oldSession.dropAndReplaceConnection(mqttConnection);
            if (!didReplace) {
                // this could happen if two clients connect at the same time with the same client ID
                throw new SessionCorruptedException("old connection replaced by different thread");
            }
        }

        return new SessionCreationResult(oldSession, CreationModeEnum.REOPEN_EXISTING, true);
    }

    private void terminateSession(Session session) {
        unsubscribe(session);
        session.release();
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
        final Queue<SessionRegistry.EnqueuedMessage> sessionQueue = queueRepository.createQueue(clientId, clean);
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
        if (pool.remove(session.getClientID(), session)) {
            session.release();
        }
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

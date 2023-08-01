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
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.moquette.broker.Session.INFINITE_EXPIRY;

public class SessionRegistry {

    private final ScheduledFuture<?> scheduledExpiredSessions;
    private int globalExpirySeconds;
    private final SessionEventLoopGroup loopsGroup;
    static final Duration EXPIRED_SESSION_CLEANER_TASK_INTERVAL = Duration.ofSeconds(1);

    public abstract static class EnqueuedMessage {

        /**
         * Releases any held resources. Must be called when the EnqueuedMessage is no
         * longer needed.
         */
        public void release() {
        }

        /**
         * Retains any held resources. Must be called when the EnqueuedMessage is added
         * to a store.
         */
        public void retain() {
        }
    }

    public static class PublishedMessage extends EnqueuedMessage {

        final Topic topic;
        final MqttQoS publishingQos;
        final ByteBuf payload;
        final boolean retained;

        public PublishedMessage(Topic topic, MqttQoS publishingQos, ByteBuf payload, boolean retained) {
            this.topic = topic;
            this.publishingQos = publishingQos;
            this.payload = payload;
            this.retained = false;
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
    private final ISessionsRepository sessionsRepository;
    private final IQueueRepository queueRepository;
    private final Authorizator authorizator;
    private final DelayQueue<ISessionsRepository.SessionData> removableSessions = new DelayQueue<>();
    private final Clock clock;

    // Used in testing
    SessionRegistry(ISubscriptionsDirectory subscriptionsDirectory,
                    ISessionsRepository sessionsRepository,
                    IQueueRepository queueRepository,
                    Authorizator authorizator,
                    ScheduledExecutorService scheduler,
                    SessionEventLoopGroup loopsGroup) {
        this(subscriptionsDirectory, sessionsRepository, queueRepository, authorizator, scheduler, Clock.systemDefaultZone(), INFINITE_EXPIRY, loopsGroup);
    }

    SessionRegistry(ISubscriptionsDirectory subscriptionsDirectory,
                    ISessionsRepository sessionsRepository,
                    IQueueRepository queueRepository,
                    Authorizator authorizator,
                    ScheduledExecutorService scheduler,
                    Clock clock, int globalExpirySeconds,
                    SessionEventLoopGroup loopsGroup) {
        this.subscriptionsDirectory = subscriptionsDirectory;
        this.sessionsRepository = sessionsRepository;
        this.queueRepository = queueRepository;
        this.authorizator = authorizator;
        this.scheduledExpiredSessions = scheduler.scheduleWithFixedDelay(this::checkExpiredSessions,
            EXPIRED_SESSION_CLEANER_TASK_INTERVAL.getSeconds(), EXPIRED_SESSION_CLEANER_TASK_INTERVAL.getSeconds(),
            TimeUnit.SECONDS);
        this.clock = clock;
        this.globalExpirySeconds = globalExpirySeconds;
        this.loopsGroup = loopsGroup;
        recreateSessionPool();
    }

    private void checkExpiredSessions() {
        List<ISessionsRepository.SessionData> expiredSessions = new ArrayList<>();
        int drainedSessions = removableSessions.drainTo(expiredSessions);
        LOG.debug("Retrieved {} expired sessions or {}", drainedSessions, removableSessions.size());
        for (ISessionsRepository.SessionData expiredSession : expiredSessions) {
            final String expiredAt = expiredSession.expireAt().map(Instant::toString).orElse("UNDEFINED");
            LOG.debug("Removing session {}, expired on {}", expiredSession.clientId(), expiredAt);
            remove(expiredSession.clientId());
            sessionsRepository.delete(expiredSession);
        }
    }

    private void trackForRemovalOnExpiration(ISessionsRepository.SessionData session) {
        if (!session.expireAt().isPresent()) {
            throw new RuntimeException("Can't track for expiration a session without expiry instant, client_id: " + session.clientId());
        }
        LOG.debug("start tracking the session {} for removal", session.clientId());
        removableSessions.add(session);
    }

    private void untrackFromRemovalOnExpiration(ISessionsRepository.SessionData session) {
        removableSessions.remove(session);
    }

    private void recreateSessionPool() {
        final Set<String> queues = queueRepository.listQueueNames();
        for (ISessionsRepository.SessionData session : sessionsRepository.list()) {
            // if the subscriptions are present is obviously false
            if (queueRepository.containsQueue(session.clientId())) {
                final SessionMessageQueue<EnqueuedMessage> persistentQueue = queueRepository.getOrCreateQueue(session.clientId());
                queues.remove(session.clientId());
                Session rehydrated = new Session(session, false, persistentQueue);
                pool.put(session.clientId(), rehydrated);
                trackForRemovalOnExpiration(session);
            }
        }
        if (!queues.isEmpty()) {
            LOG.error("Recreating sessions left {} unused queues. This is probably a bug. Session IDs: {}", queues.size(), Arrays.toString(queues.toArray()));
        }
    }

    SessionCreationResult createOrReopenSession(MqttConnectMessage msg, String clientId, String username) {
        SessionCreationResult postConnectAction;
        final Session oldSession = retrieve(clientId);
        if (oldSession == null) {
            // case 1, no existing session with given clientId.
            final Session newSession = createNewSession(msg, clientId);
            postConnectAction = new SessionCreationResult(newSession, CreationModeEnum.CREATED_CLEAN_NEW, false);

            // publish the session
            final Session previous = pool.put(clientId, newSession);
            if (previous != null) {
                // if this happens mean that another Session Event Loop thread processed a CONNECT message
                // with the same clientId. This is a bug because all messages for the same clientId should
                // be handled by the same event loop thread.
                LOG.error("Another thread added a Session for our clientId {}, this is a bug!", clientId);
            }

            LOG.trace("case 1, not existing session with CId {}", clientId);
        } else {
            postConnectAction = reopenExistingSession(msg, clientId, oldSession, username);
        }
        return postConnectAction;
    }

    private SessionCreationResult reopenExistingSession(MqttConnectMessage msg, String clientId,
                                                        Session oldSession, String username) {
        final boolean newIsClean = msg.variableHeader().isCleanSession();
        final SessionCreationResult creationResult;
        if (!oldSession.disconnected()) {
            oldSession.closeImmediately();
        }

        if (newIsClean) {
            // case 2, reopening existing session but with a clean session
            purgeSessionState(oldSession);
            // publish new session
            final Session newSession = createNewSession(msg, clientId);
            pool.put(clientId, newSession);

            LOG.trace("case 2, oldSession with same CId {} disconnected", clientId);
            creationResult = new SessionCreationResult(newSession, CreationModeEnum.CREATED_CLEAN_NEW, true);
        } else {
            final boolean connecting = oldSession.assignState(SessionStatus.DISCONNECTED, SessionStatus.CONNECTING);
            if (!connecting) {
                throw new SessionCorruptedException("old session moved in connected state by other thread");
            }
            // case 3, reopening existing session not clean session, so keep the existing subscriptions
            copySessionConfig(msg, oldSession);
            reactivateSubscriptions(oldSession, username);

            LOG.trace("case 3, oldSession with same CId {} disconnected", clientId);
            creationResult = new SessionCreationResult(oldSession, CreationModeEnum.REOPEN_EXISTING, true);
        }

        untrackFromRemovalOnExpiration(creationResult.session.getSessionData());

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
        final Session newSession;
        final SessionMessageQueue<EnqueuedMessage> queue;
        if (!clean) {
            queue = queueRepository.getOrCreateQueue(clientId);
        } else {
            queue = new InMemoryQueue();
        }
        final int expiryInterval;
        final MqttVersion mqttVersion = Utils.versionFromConnect(msg);
        if (mqttVersion != MqttVersion.MQTT_5) {
            // in MQTT3 cleanSession = true means expiryInterval=0 else infinite
            expiryInterval = clean ? 0 : globalExpirySeconds;
        } else {
            final MqttProperties.MqttProperty<Integer> expiryIntervalProperty =
                (MqttProperties.MqttProperty<Integer>) msg.variableHeader().properties()
                    .getProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value());
            if (expiryIntervalProperty != null) {
                int preferredExpiryInterval = expiryIntervalProperty.value();
                // limit the maximum expiry, use the value configured in "persistent_client_expiration"
                expiryInterval = Math.min(preferredExpiryInterval, globalExpirySeconds);
            } else {
                // the connect doesn't provide any expiry, fallback to global expiry
                expiryInterval = clean ? 0 : globalExpirySeconds;
            }
        }
        final ISessionsRepository.SessionData sessionData = new ISessionsRepository.SessionData(clientId,
            mqttVersion, expiryInterval, clock);
        if (msg.variableHeader().isWillFlag()) {
            final Session.Will will = createWill(msg);
            newSession = new Session(sessionData, clean, will, queue);
        } else {
            newSession = new Session(sessionData, clean, queue);
        }

        newSession.markConnecting();
        sessionsRepository.saveSession(sessionData);
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

    void connectionClosed(Session session) {
        session.disconnect();
        if (session.expireImmediately()) {
            purgeSessionState(session);
        } else {
            //bound session has expiry, disconnect it and add to the queue for removal
            trackForRemovalOnExpiration(session.getSessionData().withExpirationComputed());
        }
    }

    private void purgeSessionState(Session session) {
        LOG.debug("Remove session state for client {}", session.getClientID());
        boolean result = session.assignState(SessionStatus.DISCONNECTED, SessionStatus.DESTROYED);
        if (!result) {
            throw new SessionCorruptedException("Session has already changed state: " + session);
        }

        unsubscribe(session);
        remove(session.getClientID());
    }

    void remove(String clientID) {
        final Session old = pool.remove(clientID);
        if (old != null) {
            // remove from expired tracker if present
            removableSessions.remove(old.getSessionData());
            loopsGroup.routeCommand(clientID, "Clean up removed session", () -> {
                old.cleanUp();
                return null;
            });
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

   /**
    * Close the connection bound to the session for the clintId. If removeSessionState is provided
    * remove any session state like queues and subscription from broker memory.
    *
    * @param clientId the name of the client to drop the session.
    * @param removeSessionState boolean flag to request the removal of session state from broker.
    */ 
    boolean dropSession(final String clientId, boolean removeSessionState) {
        LOG.debug("Disconnecting client: {}", clientId);
        if (clientId == null) {
            return false;
        }

        final Session client = pool.get(clientId);
        if (client == null) {
            LOG.debug("Client {} not found, nothing disconnected", clientId);
            return false;
        }

        client.closeImmediately();
        if (removeSessionState) {
            purgeSessionState(client);
        }

       LOG.debug("Client {} successfully disconnected from broker", clientId);
       return true;
    }

    private Optional<ClientDescriptor> createClientDescriptor(Session s) {
        final String clientID = s.getClientID();
        final Optional<InetSocketAddress> remoteAddressOpt = s.remoteAddress();
        return remoteAddressOpt.map(r -> new ClientDescriptor(clientID, r.getHostString(), r.getPort()));
    }

    /**
     * Close all resources related to session management
     */
    public void close() {
        if (scheduledExpiredSessions.cancel(false)) {
            LOG.info("Successfully cancelled expired sessions task");
        } else {
            LOG.warn("Can't cancel the execution of expired sessions task, was already cancelled? {}, was done? {}",
                scheduledExpiredSessions.isCancelled(), scheduledExpiredSessions.isDone());
        }
        // Update all not clean session with the proper expiry date
        updateNotCleanSessionsWithProperExpire();
        queueRepository.close();
    }

    private void updateNotCleanSessionsWithProperExpire() {
        pool.values().stream()
            .filter(s -> !s.isClean()) // not clean session
            .map(Session::getSessionData)
            .filter(s -> !s.expireAt().isPresent()) // without expire set
            .map(ISessionsRepository.SessionData::withExpirationComputed) // new SessionData with expireAt valued
            .forEach(sessionsRepository::saveSession); // update the storage
    }
}

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
import io.moquette.broker.scheduler.ScheduledExpirationService;
import io.moquette.broker.subscriptions.ISubscriptionsDirectory;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.moquette.BrokerConstants.INFLIGHT_WINDOW_SIZE;
import static io.moquette.broker.Session.INFINITE_EXPIRY;

public class SessionRegistry {

    private int globalExpirySeconds;
    private final SessionEventLoopGroup loopsGroup;
    static final Duration EXPIRED_SESSION_CLEANER_TASK_INTERVAL = Duration.ofSeconds(1);
    private ScheduledExpirationService<ISessionsRepository.SessionData> sessionExpirationService;

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
        final Instant messageExpiry;
        final MqttProperties.MqttProperty[] mqttProperties;

        public PublishedMessage(Topic topic, MqttQoS publishingQos, ByteBuf payload, boolean retained,
                                Instant messageExpiry, MqttProperties.MqttProperty... mqttProperties) {
            this.topic = topic;
            this.publishingQos = publishingQos;
            this.payload = payload;
            this.retained = retained; // TODO has to store retained param into the field
            this.messageExpiry = messageExpiry;
            this.mqttProperties = mqttProperties;
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

        public MqttProperties.MqttProperty[] getMqttProperties() {
            return mqttProperties;
        }

        public boolean isExpired() {
            return messageExpiry != Instant.MAX && Instant.now().isAfter(messageExpiry);
        }

        public MqttProperties.MqttProperty[] updatePublicationExpiryIfPresentOrAdd() {
            if (messageExpiry == Instant.MAX) {
                return mqttProperties;
            }

            Duration duration = Duration.between(Instant.now(), messageExpiry);
            // do some math rounding so that 2.9999 seconds remains 3 seconds
            long remainingSeconds = Math.round(duration.toMillis() / 1_000.0);
            final int indexOfExpiry = findPublicationExpiryProperty(mqttProperties);
            MqttProperties.IntegerProperty updatedProperty = new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value(), (int) remainingSeconds);

            // update existing property
            if (indexOfExpiry != -1) {
                mqttProperties[indexOfExpiry] = updatedProperty;
                return mqttProperties;
            }

            // insert a new property
            MqttProperties.MqttProperty[] newProperties = Arrays.copyOf(mqttProperties, mqttProperties.length + 1);
            newProperties[newProperties.length - 1] = updatedProperty;
            return newProperties;
        }

        /**
         * Linear search of PUBLICATION_EXPIRY_INTERVAL.
         * @param properties the array of properties.
         * @return the index of matched property or -1.
         * */
        private static int findPublicationExpiryProperty(MqttProperties.MqttProperty[] properties) {
            for (int i = 0; i < properties.length; i++) {
                if (isPublicationExpiryProperty(properties[i])) {
                    return i;
                }
            }
            return -1;
        }

        private static boolean isPublicationExpiryProperty(MqttProperties.MqttProperty property) {
            return property instanceof MqttProperties.IntegerProperty &&
                property.propertyId() == MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value();
        }

        public Instant getMessageExpiry() {
            return messageExpiry;
        }

        @Override
        public String toString() {
            return "PublishedMessage{" +
                "topic=" + topic +
                ", publishingQos=" + publishingQos +
                ", payload=" + payload +
                ", retained=" + retained +
                ", messageExpiry=" + messageExpiry +
                ", mqttProperties=" + Arrays.toString(mqttProperties) +
                '}';
        }
    }

    public static final class PubRelMarker extends EnqueuedMessage {
        @Override
        public String toString() {
            return "PubRelMarker{}";
        }
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
        sessionExpirationService = new ScheduledExpirationService<>(clock, this::removeExpiredSession);
        this.clock = clock;
        this.globalExpirySeconds = globalExpirySeconds;
        this.loopsGroup = loopsGroup;
        recreateSessionPool();
    }

    private void removeExpiredSession(ISessionsRepository.SessionData expiredSession) {
        final String expiredAt = expiredSession.expireAt().map(Instant::toString).orElse("UNDEFINED");
        LOG.debug("Removing session {}, expired on {}", expiredSession.clientId(), expiredAt);
        remove(expiredSession.clientId());
        sessionsRepository.delete(expiredSession);

        subscriptionsDirectory.removeSharedSubscriptionsForClient(expiredSession.clientId());
    }

    private void trackForRemovalOnExpiration(ISessionsRepository.SessionData session) {
        LOG.debug("start tracking the session {} for removal", session.clientId());
        sessionExpirationService.track(session.clientId(), session);
    }

    private void untrackFromRemovalOnExpiration(ISessionsRepository.SessionData session) {
        sessionExpirationService.untrack(session.clientId());
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
            boolean hasWillFlag = msg.variableHeader().isWillFlag();
            ISessionsRepository.SessionData newSessionData = oldSession.getSessionData();
            if (hasWillFlag) {
                newSessionData = newSessionData.withWill(createNewWill(msg));
            } else {
                newSessionData = newSessionData.withoutWill();
            }
            oldSession.updateSessionData(newSessionData);
            oldSession.markAsNotClean();

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
        final ISessionsRepository.SessionData sessionData;
        if (msg.variableHeader().isWillFlag()) {
            final ISessionsRepository.Will will = createNewWill(msg);
            sessionData = new ISessionsRepository.SessionData(clientId, mqttVersion, will, expiryInterval, clock);
        } else {
            sessionData = new ISessionsRepository.SessionData(clientId, mqttVersion, expiryInterval, clock);
        }

        newSession = new Session(sessionData, clean, queue);
        newSession.markConnecting();
        sessionsRepository.saveSession(sessionData);
        if (MQTTConnection.isNeedResponseInformation(msg)) {
            // the responder client must have write access to this topic
            // the requester client must have read access on this topic
            authorizator.forceReadAccess(Topic.asTopic("/reqresp/response/" + clientId), clientId);
            authorizator.forceWriteToAll(Topic.asTopic("/reqresp/response/" + clientId));
        }
        return newSession;
    }

    private ISessionsRepository.Will createNewWill(MqttConnectMessage msg) {
        final byte[] willPayload = msg.payload().willMessageInBytes();
        final String willTopic = msg.payload().willTopic();
        final boolean retained = msg.variableHeader().isWillRetain();
        final MqttQoS qos = MqttQoS.valueOf(msg.variableHeader().willQos());

        if (Utils.versionFromConnect(msg) != MqttVersion.MQTT_5) {
            return new ISessionsRepository.Will(willTopic, willPayload, qos, retained, 0);
        }

        // retrieve Will Delay if present and if the connection is MQTT5
        final int willDelayIntervalSeconds;
        MqttProperties willProperties = msg.payload().willProperties();
        final MqttProperties.MqttProperty<Integer> willDelayIntervalProperty =
            willProperties.getProperty(MqttProperties.MqttPropertyType.WILL_DELAY_INTERVAL.value());
        if (willDelayIntervalProperty != null) {
            willDelayIntervalSeconds = willDelayIntervalProperty.value();
        } else {
            willDelayIntervalSeconds = 0;
        }
        ISessionsRepository.Will will = new ISessionsRepository.Will(willTopic, willPayload, qos, retained, willDelayIntervalSeconds);

        ISessionsRepository.WillOptions options = ISessionsRepository.WillOptions.empty();
        MqttProperties.MqttProperty<Integer> messageExpiryIntervalProperty =
            willProperties.getProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value());
        if (messageExpiryIntervalProperty != null) {
            Integer messageExpiryIntervalSeconds = messageExpiryIntervalProperty.value();
            options = options.withMessageExpiry(Duration.ofSeconds(messageExpiryIntervalSeconds));
        }

        MqttProperties.MqttProperty<String> contentTypeProperty =
            willProperties.getProperty(MqttProperties.MqttPropertyType.CONTENT_TYPE.value());
        if (contentTypeProperty != null) {
            options = options.withContentType(contentTypeProperty.value());
        }

        MqttProperties.MqttProperty<String> responseTopicProperty =
            willProperties.getProperty(MqttProperties.MqttPropertyType.RESPONSE_TOPIC.value());
        if (responseTopicProperty != null) {
            options = options.withResponseTopic(responseTopicProperty.value());
        }

        MqttProperties.MqttProperty<byte[]> correlationDataProperty =
            willProperties.getProperty(MqttProperties.MqttPropertyType.CORRELATION_DATA.value());
        if (correlationDataProperty != null) {
            options = options.withCorrelationData(correlationDataProperty.value());
        }

        List<? extends MqttProperties.MqttProperty> userProperties = willProperties.getProperties(MqttProperties.MqttPropertyType.USER_PROPERTY.value());
        if (userProperties != null && !userProperties.isEmpty()) {
            Map<String, String> props = new HashMap<>(userProperties.size());
            for (MqttProperties.UserProperty userProperty: (List<MqttProperties.UserProperty>) userProperties) {
                props.put(userProperty.value().key, userProperty.value().value);
            }
            options = options.withUserProperties(props);
        }
        if (options.notEmpty()) {
            return new ISessionsRepository.Will(will, options);
        } else {
            return will;
        }
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
            ISessionsRepository.SessionData sessionData = session.getSessionData().withExpirationComputed();
            trackForRemovalOnExpiration(sessionData);
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

        subscriptionsDirectory.removeSharedSubscriptionsForClient(session.getClientID());
    }

    void remove(String clientID) {
        final Session old = pool.remove(clientID);
        if (old != null) {
            // remove from expired tracker if present
            sessionExpirationService.untrack(clientID);
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
        sessionExpirationService.shutdown();
        // Update all not clean session with the proper expiry date
        updateNotCleanSessionsWithProperExpire();
        queueRepository.close();
        pool.values().forEach(Session::cleanUp);
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

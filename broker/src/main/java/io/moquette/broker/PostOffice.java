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

import io.moquette.interception.BrokerInterceptor;
import io.moquette.broker.subscriptions.ISubscriptionsDirectory;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.interception.messages.InterceptAcknowledgedMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.moquette.broker.Utils.messageId;
import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttQoS.*;

class PostOffice {
    private static final int ZERO_PACKET_ID_USE_AUTOGENERATION = 0;
    /**
     * Maps the failed packetID per clientId (id client source, id_packet) -> [id client target]
     * */
    private static class FailedPublishCollection {

        static class PacketId {

            private final String clientId;
            private final int idPacket;

            PacketId(String clientId, int idPacket) {
                this.clientId = clientId;
                this.idPacket = idPacket;
            }
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                PacketId packetId = (PacketId) o;
                return idPacket == packetId.idPacket && Objects.equals(clientId, packetId.clientId);
            }

            @Override
            public int hashCode() {
                return Objects.hash(clientId, idPacket);
            }

            public boolean belongToClient(String clientId) {
                return this.clientId.equals(clientId);
            }
        }

        private final ConcurrentMap<PacketId, Set<String>> packetsMap = new ConcurrentHashMap<>();

        private void insert(String clientId, int messageID, String failedClientId) {
            final PacketId packetId = new PacketId(clientId, messageID);
            packetsMap.computeIfAbsent(packetId, k -> new HashSet<>())
                .add(failedClientId);
        }

        public void remove(String clientId, int messageID, String targetClientId) {
            final PacketId packetId = new PacketId(clientId, messageID);
            packetsMap.computeIfPresent(packetId, (key, clientsSet) -> {
               clientsSet.remove(targetClientId);
               if (clientsSet.isEmpty()) {
                   // the mapping key, value is removed
                   return null;
               } else {
                   return clientsSet;
               }
            });
        }

        private void removeAll(int messageID, String clientId, Collection<String> routings) {
            for (String targetClientId : routings) {
                remove(clientId, messageID, targetClientId);
            }
        }

        void cleanupForClient(String clientId) {
            // the only way to linear scan the map to collect of PacketIds references,
            // for a following step of cleaning
            packetsMap.keySet().stream()
                .filter(packet -> packet.belongToClient(clientId))
                .forEach(packetsMap::remove);
        }

        void insertAll(int messageID, String clientId, Collection<String> routings) {
            for (String targetClientId : routings) {
                insert(clientId, messageID, targetClientId);
            }
        }

        Set<String> listFailed(String clientId, int messageID) {
            final PacketId packetId = new PacketId(clientId, messageID);
            return packetsMap.getOrDefault(packetId, Collections.emptySet());
        }
    }

    static class RouteResult {
        private final String clientId;
        private final Status status;
        private CompletableFuture queuedFuture;

        enum Status {SUCCESS, FAIL}

        public static RouteResult success(String clientId, CompletableFuture queuedFuture) {
            return new RouteResult(clientId, Status.SUCCESS, queuedFuture);
        }

        public static RouteResult failed(String clientId) {
            return failed(clientId, null);
        }

        public static RouteResult failed(String clientId, String error) {
           final CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new Error(error));
            return new RouteResult(clientId, Status.FAIL, failed);
        }

        private RouteResult(String clientId, Status status, CompletableFuture queuedFuture) {
            this.clientId = clientId;
            this.status = status;
            this.queuedFuture = queuedFuture;
        }

        public CompletableFuture completableFuture() {
            if (status == Status.FAIL) {
                throw new IllegalArgumentException("Accessing completable future on a failed result");
            }
            return queuedFuture;
        }

        public boolean isSuccess() {
            return status == Status.SUCCESS;
        }

        public RouteResult ifFailed(Runnable action) {
            if (!isSuccess()) {
                action.run();
            }
            return this;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PostOffice.class);

    private static final Set<String> NO_FILTER = new HashSet<>();

    private final Authorizator authorizator;
    private final ISubscriptionsDirectory subscriptions;
    private final IRetainedRepository retainedRepository;
    private SessionRegistry sessionRegistry;
    private BrokerInterceptor interceptor;

    private final Thread[] sessionExecutors;
    private final BlockingQueue<FutureTask<String>>[] sessionQueues;
    private final int eventLoops = Runtime.getRuntime().availableProcessors();
    private final FailedPublishCollection failedPublishes = new FailedPublishCollection();

    PostOffice(ISubscriptionsDirectory subscriptions, IRetainedRepository retainedRepository,
               SessionRegistry sessionRegistry, BrokerInterceptor interceptor, Authorizator authorizator, int sessionQueueSize) {
        this.authorizator = authorizator;
        this.subscriptions = subscriptions;
        this.retainedRepository = retainedRepository;
        this.sessionRegistry = sessionRegistry;
        this.interceptor = interceptor;

        this.sessionQueues = new BlockingQueue[eventLoops];
        for (int i = 0; i < eventLoops; i++) {
            this.sessionQueues[i] = new ArrayBlockingQueue<>(sessionQueueSize);
        }
        this.sessionExecutors = new Thread[eventLoops];
        for (int i = 0; i < eventLoops; i++) {
            this.sessionExecutors[i] = new Thread(new SessionEventLoop(this.sessionQueues[i]));
            this.sessionExecutors[i].setName("Session Executor " + i);
            this.sessionExecutors[i].start();
        }
    }

    public void init(SessionRegistry sessionRegistry) {
        this.sessionRegistry = sessionRegistry;
    }

    public void fireWill(Session.Will will) {
        // MQTT 3.1.2.8-17
        publish2Subscribers(will.payload, new Topic(will.topic), will.qos);
    }

    public void subscribeClientToTopics(MqttSubscribeMessage msg, String clientID, String username,
                                        MQTTConnection mqttConnection) {
        // verify which topics of the subscribe ongoing has read access permission
        int messageID = messageId(msg);
        List<MqttTopicSubscription> ackTopics = authorizator.verifyTopicsReadAccess(clientID, username, msg);
        MqttSubAckMessage ackMessage = doAckMessageFromValidateFilters(ackTopics, messageID);

        // store topics subscriptions in session
        List<Subscription> newSubscriptions = ackTopics.stream()
            .filter(req -> req.qualityOfService() != FAILURE)
            .map(req -> {
                final Topic topic = new Topic(req.topicName());
                return new Subscription(clientID, topic, req.qualityOfService());
            }).collect(Collectors.toList());

        for (Subscription subscription : newSubscriptions) {
            subscriptions.add(subscription);
        }

        // add the subscriptions to Session
        Session session = sessionRegistry.retrieve(clientID);
        session.addSubscriptions(newSubscriptions);

        // send ack message
        mqttConnection.sendSubAckMessage(messageID, ackMessage);

        publishRetainedMessagesForSubscriptions(clientID, newSubscriptions);

        for (Subscription subscription : newSubscriptions) {
            interceptor.notifyTopicSubscribed(subscription, username);
        }
    }

    private void publishRetainedMessagesForSubscriptions(String clientID, List<Subscription> newSubscriptions) {
        Session targetSession = this.sessionRegistry.retrieve(clientID);
        for (Subscription subscription : newSubscriptions) {
            final String topicFilter = subscription.getTopicFilter().toString();
            final List<RetainedMessage> retainedMsgs = retainedRepository.retainedOnTopic(topicFilter);

            if (retainedMsgs.isEmpty()) {
                // not found
                continue;
            }
            for (RetainedMessage retainedMsg : retainedMsgs) {
                final MqttQoS retainedQos = retainedMsg.qosLevel();
                MqttQoS qos = lowerQosToTheSubscriptionDesired(subscription, retainedQos);

                final ByteBuf payloadBuf = Unpooled.wrappedBuffer(retainedMsg.getPayload());
                targetSession.sendRetainedPublishOnSessionAtQos(retainedMsg.getTopic(), qos, payloadBuf);
                // We made the buffer, we must release it.
                payloadBuf.release();
            }
        }
    }

    /**
     * Create the SUBACK response from a list of topicFilters
     */
    private MqttSubAckMessage doAckMessageFromValidateFilters(List<MqttTopicSubscription> topicFilters, int messageId) {
        List<Integer> grantedQoSLevels = new ArrayList<>();
        for (MqttTopicSubscription req : topicFilters) {
            grantedQoSLevels.add(req.qualityOfService().value());
        }

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, AT_MOST_ONCE,
            false, 0);
        MqttSubAckPayload payload = new MqttSubAckPayload(grantedQoSLevels);
        return new MqttSubAckMessage(fixedHeader, from(messageId), payload);
    }

    public void unsubscribe(List<String> topics, MQTTConnection mqttConnection, int messageId) {
        final String clientID = mqttConnection.getClientId();
        final Session session = sessionRegistry.retrieve(clientID);
        if (session == null) {
            // Session is already destroyed.
            // Just ack the client
            LOG.warn("Session not found when unsubscribing {}", clientID);
            mqttConnection.sendUnsubAckMessage(topics, clientID, messageId);
            return;
        }
        for (String t : topics) {
            Topic topic = new Topic(t);
            boolean validTopic = topic.isValid();
            if (!validTopic) {
                // close the connection, not valid topicFilter is a protocol violation
                mqttConnection.dropConnection();
                LOG.warn("Topic filter is not valid. topics: {}, offending topic filter: {}", topics, topic);
                return;
            }

            LOG.trace("Removing subscription topic={}", topic);
            subscriptions.removeSubscription(topic, clientID);

            session.removeSubscription(topic);

            String username = NettyUtils.userName(mqttConnection.channel);
            interceptor.notifyTopicUnsubscribed(topic.toString(), clientID, username);
        }

        // ack the client
        mqttConnection.sendUnsubAckMessage(topics, clientID, messageId);
    }

    CompletableFuture<Void> receivedPublishQos0(Topic topic, String username, String clientID, MqttPublishMessage msg) {
        if (!authorizator.canWrite(topic, username, clientID)) {
            LOG.error("client is not authorized to publish on topic: {}", topic);
            ReferenceCountUtil.release(msg);
            return CompletableFuture.completedFuture(null);
        }
        final RoutingResults publishResult = publish2Subscribers(msg.payload(), topic, AT_MOST_ONCE);
        if (publishResult.isAllFailed()) {
            LOG.info("No one publish was successfully enqueued to session loops");
            ReferenceCountUtil.release(msg);
            return CompletableFuture.completedFuture(null);
        }

        return publishResult.completableFuture().thenRun(() -> {
            if (msg.fixedHeader().isRetain()) {
                // QoS == 0 && retain => clean old retained
                retainedRepository.cleanRetained(topic);
            }

            interceptor.notifyTopicPublished(msg, clientID, username);
            ReferenceCountUtil.release(msg);
        });
    }

    RoutingResults receivedPublishQos1(MQTTConnection connection, Topic topic, String username, int messageID,
                                                MqttPublishMessage msg) {
        // verify if topic can be written
        topic.getTokens();
        if (!topic.isValid()) {
            LOG.warn("Invalid topic format, force close the connection");
            connection.dropConnection();
            ReferenceCountUtil.release(msg);
            return RoutingResults.preroutingError();
        }
        final String clientId = connection.getClientId();
        if (!authorizator.canWrite(topic, username, clientId)) {
            LOG.error("MQTT client: {} is not authorized to publish on topic: {}", clientId, topic);
            ReferenceCountUtil.release(msg);
            return RoutingResults.preroutingError();
        }

        ByteBuf payload = msg.payload();
        final RoutingResults routes;
        if (msg.fixedHeader().isDup()) {
            final Set<String> failedClients = failedPublishes.listFailed(clientId, messageID);
            routes = publish2Subscribers(payload, topic, AT_LEAST_ONCE, failedClients);
        } else {
            routes = publish2Subscribers(payload, topic, AT_LEAST_ONCE);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("subscriber routes: {}", routes);
        }
        if (routes.isAllSuccess()) {
            // QoS1 message was enqueued successfully to every event loop
            connection.sendPubAck(messageID);
            manageRetain(topic, msg);
            interceptor.notifyTopicPublished(msg, clientId, username);
        } else {
            // some session event loop enqueue raised a problem
            failedPublishes.insertAll(messageID, clientId, routes.failedRoutings);
        }
        ReferenceCountUtil.release(msg);

        // cleanup success resends from the failed publishes cache
        failedPublishes.removeAll(messageID, clientId, routes.successedRoutings);

        return routes;
    }

    private void manageRetain(Topic topic, MqttPublishMessage msg) {
        if (msg.fixedHeader().isRetain()) {
            if (!msg.payload().isReadable()) {
                retainedRepository.cleanRetained(topic);
            } else {
                // before wasn't stored
                retainedRepository.retain(topic, msg);
            }
        }
    }

    private RoutingResults publish2Subscribers(ByteBuf payload, Topic topic, MqttQoS publishingQos, Set<String> filterTargetClients) {
        return publish2Subscribers(payload, topic, publishingQos, filterTargetClients, ZERO_PACKET_ID_USE_AUTOGENERATION);
    }

    private RoutingResults publish2Subscribers(ByteBuf payload, Topic topic, MqttQoS publishingQos) {
        return publish2Subscribers(payload, topic, publishingQos, NO_FILTER, ZERO_PACKET_ID_USE_AUTOGENERATION);
    }

    private class BatchingPublishesCollector {
        final List<Subscription>[] subscriptions;
        private final int eventLoops;

        BatchingPublishesCollector(int eventLoops) {
            subscriptions = new List[eventLoops];
            this.eventLoops = eventLoops;
        }

        public void add(Subscription sub) {
            final int targetQueueId = subscriberEventLoop(sub.getClientId());
            if (subscriptions[targetQueueId] == null) {
                subscriptions[targetQueueId] = new ArrayList<>();
            }
            subscriptions[targetQueueId].add(sub);
        }

        private int subscriberEventLoop(String clientId) {
            return Math.abs(clientId.hashCode()) % this.eventLoops;
        }

        List<RouteResult> routeBatchedPublishes(Consumer<List<Subscription>> action) {
            List<RouteResult> publishResults = new ArrayList<>(this.eventLoops);

            for (List<Subscription> subscriptionsBatch : subscriptions) {
                if (subscriptionsBatch == null) {
                    continue;
                }
                final String clientId = subscriptionsBatch.get(0).getClientId();
                if (LOG.isTraceEnabled()) {
                    final String subscriptionsDetails = subscriptionsBatch.stream()
                        .map(Subscription::toString)
                        .collect(Collectors.joining(",\n"));
                    final int loopId = subscriberEventLoop(clientId);
                    LOG.trace("Routing PUBLISH to eventLoop {}  for subscriptions [{}]", loopId, subscriptionsDetails);
                }
                publishResults.add(routeCommand(clientId, "batched PUB", () -> {
                    action.accept(subscriptionsBatch);
                    return null;
                }));
            }
            return publishResults;
        }

        Collection<String> subscriberIdsByEventLoop(String clientId) {
            final int targetQueueId = subscriberEventLoop(clientId);
            return subscriptions[targetQueueId].stream().map(Subscription::getClientId).collect(Collectors.toList());
        }

        public int countBatches() {
            int count = 0;
            for (List<Subscription> subscriptionsBatch : subscriptions) {
                if (subscriptionsBatch == null) {
                    continue;
                }
                count ++;
            }
            return count;
        }
    }

    private RoutingResults publish2Subscribers(ByteBuf payload, Topic topic, MqttQoS publishingQos,
                                               Set<String> filterTargetClients, int packetId) {
        Set<Subscription> topicMatchingSubscriptions = subscriptions.matchQosSharpening(topic);
        if (topicMatchingSubscriptions.isEmpty()) {
            // no matching subscriptions, clean exit
            LOG.trace("No matching subscriptions for topic: {}", topic);
            return new RoutingResults(Collections.emptyList(), Collections.emptyList(), CompletableFuture.completedFuture(null));
        }

        final BatchingPublishesCollector collector = new BatchingPublishesCollector(eventLoops);

        for (final Subscription sub : topicMatchingSubscriptions) {
            if (filterTargetClients == NO_FILTER || filterTargetClients.contains(sub.getClientId())) {
                collector.add(sub);
            }
        }
        payload.retain(collector.countBatches());

        List<RouteResult> publishResults = collector.routeBatchedPublishes((batch) -> {
            publishToSession(payload, topic, batch, publishingQos, packetId); // iqm
            payload.release();
        });

        final CompletableFuture[] publishFutures = publishResults.stream()
            .filter(RouteResult::isSuccess)
            .map(RouteResult::completableFuture).toArray(CompletableFuture[]::new);
        final CompletableFuture<Void> publishes = CompletableFuture.allOf(publishFutures);

        final List<String> failedRoutings = new ArrayList<>();
        final List<String> successedRoutings = new ArrayList<>();
        for (RouteResult rr : publishResults) {
            Collection<String> subscibersIds = collector.subscriberIdsByEventLoop(rr.clientId);
            if (rr.status == RouteResult.Status.FAIL) {
                failedRoutings.addAll(subscibersIds);
                payload.release();
            } else {
                successedRoutings.addAll(subscibersIds);
            }
        }
        return new RoutingResults(successedRoutings, failedRoutings, publishes);
    }

    private void publishToSession(ByteBuf payload, Topic topic, Collection<Subscription> subscriptions, MqttQoS publishingQos, int packetId) {
        for (Subscription sub : subscriptions) {
            MqttQoS qos = lowerQosToTheSubscriptionDesired(sub, publishingQos);
            publishToSession(payload, topic, sub, qos, packetId); // iqm
        }
    }

    private void publishToSession(ByteBuf payload, Topic topic, Subscription sub, MqttQoS qos, int packetId) {
        Session targetSession = this.sessionRegistry.retrieve(sub.getClientId());

        boolean isSessionPresent = targetSession != null;
        if (isSessionPresent) {
            LOG.debug("Sending PUBLISH message to active subscriber CId: {}, topicFilter: {}, qos: {}",
                      sub.getClientId(), sub.getTopicFilter(), qos);
            targetSession.sendNotRetainedPublishOnSessionAtQosWithPackId(topic, qos, payload, packetId); // iqm
        } else {
            // If we are, the subscriber disconnected after the subscriptions tree selected that session as a
            // destination.
            LOG.debug("PUBLISH to not yet present session. CId: {}, topicFilter: {}, qos: {}", sub.getClientId(),
                      sub.getTopicFilter(), qos);
        }
    }

    /**
     * First phase of a publish QoS2 protocol, sent by publisher to the broker. Publish to all interested
     * subscribers.
     * @return
     */
    RoutingResults receivedPublishQos2(MQTTConnection connection, MqttPublishMessage msg, String username) {
        LOG.trace("Processing PUB QoS2 message on connection: {}", connection);
        final Topic topic = new Topic(msg.variableHeader().topicName());
        final ByteBuf payload = msg.payload();

        final String clientId = connection.getClientId();
        if (!authorizator.canWrite(topic, username, clientId)) {
            LOG.error("MQTT client is not authorized to publish on topic: {}", topic);
            ReferenceCountUtil.release(msg);
            // WARN this is a special case failed is empty, but this result is to be considered as error.
            return RoutingResults.preroutingError();
        }

        final int messageID = msg.variableHeader().packetId();
        final RoutingResults publishRoutings;
        if (msg.fixedHeader().isDup()) {
            final Set<String> failedClients = failedPublishes.listFailed(clientId, messageID);
            publishRoutings = publish2Subscribers(payload, topic, EXACTLY_ONCE, failedClients); // iqm
        } else {
            publishRoutings = publish2Subscribers(payload, topic, EXACTLY_ONCE);
        }
        if (publishRoutings.isAllSuccess()) {
            // QoS2 PUB message was enqueued successfully to every event loop
            connection.sendPubRec(messageID);
            manageRetain(topic, msg);
            interceptor.notifyTopicPublished(msg, clientId, username);
        } else {
            // some session event loop enqueue raised a problem
            failedPublishes.insertAll(messageID, clientId, publishRoutings.failedRoutings);
        }
        ReferenceCountUtil.release(msg);

        // cleanup success resends from the failed publishes cache
        failedPublishes.removeAll(messageID, clientId, publishRoutings.successedRoutings);

        return publishRoutings;
    }

    static MqttQoS lowerQosToTheSubscriptionDesired(Subscription sub, MqttQoS qos) {
        if (qos.value() > sub.getRequestedQos().value()) {
            qos = sub.getRequestedQos();
        }
        return qos;
    }

    /**
     * Intended usage is only for embedded versions of the broker, where the hosting application
     * want to use the broker to send a publish message. Like normal external publish message but
     * with some changes to avoid security check, and the handshake phases for Qos1 and Qos2. It
     * also doesn't notifyTopicPublished because using internally the owner should already know
     * where it's publishing.
     *
     * @param msg
     *            the message to publish
     * @return
     *            the result of the enqueuing operation to session loops.
     */
    public RoutingResults internalPublish(MqttPublishMessage msg) {
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        final Topic topic = new Topic(msg.variableHeader().topicName());
        final ByteBuf payload = msg.payload();
        LOG.info("Sending internal PUBLISH message Topic={}, qos={}", topic, qos);

        final RoutingResults publishResult;
        if (msg.variableHeader().packetId() > 0) {
            publishResult = publish2Subscribers(payload, topic, qos, NO_FILTER, msg.variableHeader().packetId());
        } else {
            publishResult = publish2Subscribers(payload, topic, qos);
        }
        LOG.trace("after routed publishes: {}", publishResult);

        if (!msg.fixedHeader().isRetain()) {
            return publishResult;
        }
        if (qos == AT_MOST_ONCE || payload.readableBytes() == 0) {
            // QoS == 0 && retain => clean old retained
            retainedRepository.cleanRetained(topic);
            return publishResult;
        }
        retainedRepository.retain(topic, msg);
        return publishResult;
    }

    /**
     * notify MqttConnectMessage after connection established (already pass login).
     * @param msg
     */
    void dispatchConnection(MqttConnectMessage msg) {
        interceptor.notifyClientConnected(msg);
    }

    void dispatchDisconnection(String clientId,String userName) {
        interceptor.notifyClientDisconnected(clientId, userName);
    }

    void dispatchConnectionLost(String clientId,String userName) {
        interceptor.notifyClientConnectionLost(clientId, userName);
    }

    void dispatchMessageAcknowledgement(String clientId, int packetId) {
        interceptor.notifyMessageAcknowledged(new InterceptAcknowledgedMessage(
            null, null, clientId, packetId));
    }

    /**
     * Route the command to the owning SessionEventLoop
     * */
    public RouteResult routeCommand(String clientId, String actionDescription, Callable<String> action) {
        SessionCommand cmd = new SessionCommand(clientId, action);
        final int targetQueueId = Math.abs(cmd.getSessionId().hashCode()) % this.eventLoops;
        LOG.debug("Routing cmd [{}] for session [{}] to event processor {}", actionDescription, cmd.getSessionId(), targetQueueId);
        final FutureTask<String> task = new FutureTask<>(() -> {
            cmd.execute();
            cmd.complete();
            return cmd.getSessionId();
        });
        if (Thread.currentThread() == sessionExecutors[targetQueueId]) {
            SessionEventLoop.executeTask(task);
            return RouteResult.success(clientId, cmd.completableFuture());
        }
        if (this.sessionQueues[targetQueueId].offer(task)) {
            return RouteResult.success(clientId, cmd.completableFuture());
        } else {
            LOG.warn("Session command queue {} is full executing action {}", targetQueueId, actionDescription);
            return RouteResult.failed(clientId);
        }
    }

    public void terminate() {
        for (Thread processor : sessionExecutors) {
            processor.interrupt();
        }
        for (Thread processor : sessionExecutors) {
            try {
                processor.join(5_000);
            } catch (InterruptedException ex) {
                LOG.info("Interrupted while joining session event loop {}", processor.getName(), ex);
            }
        }
    }

    /**
     * Clean up all the data related to the specified client;
     * */
    public void clientDisconnected(String clientID, String userName) {
        dispatchDisconnection(clientID, userName);
        this.failedPublishes.cleanupForClient(clientID);
    }
}

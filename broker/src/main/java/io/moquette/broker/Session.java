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

import static io.moquette.BrokerConstants.FLIGHT_BEFORE_RESEND_MS;

import io.moquette.broker.SessionRegistry.EnqueuedMessage;
import io.moquette.broker.SessionRegistry.PublishedMessage;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class Session {

    private static final Logger LOG = LoggerFactory.getLogger(Session.class);
    // By specification session expiry value of 0xEFFFFFFF (UINT_MAX) (seconds) means
    // session that doesn't expire, it's ~68 years.
    static final int INFINITE_EXPIRY = Integer.MAX_VALUE;
    private final boolean resendInflightOnTimeout;
    private Collection<Integer> nonAckPacketIds;

    static class InFlightPacket implements Delayed {

        final int packetId;
        private long startTime;

        InFlightPacket(int packetId, long delayInMilliseconds) {
            this.packetId = packetId;
            this.startTime = System.currentTimeMillis() + delayInMilliseconds;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long diff = startTime - System.currentTimeMillis();
            return unit.convert(diff, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if ((this.startTime - ((InFlightPacket) o).startTime) == 0) {
                return 0;
            }
            if ((this.startTime - ((InFlightPacket) o).startTime) > 0) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    enum SessionStatus {
        CONNECTED, CONNECTING, DISCONNECTING, DISCONNECTED, DESTROYED
    }

    private boolean clean;
    private final SessionMessageQueue<SessionRegistry.EnqueuedMessage> sessionQueue;
    private final AtomicReference<SessionStatus> status = new AtomicReference<>(SessionStatus.DISCONNECTED);
    private MQTTConnection mqttConnection;
    private final Set<Subscription> subscriptions = new HashSet<>();
    private final Map<Integer, SessionRegistry.EnqueuedMessage> inflightWindow = new HashMap<>();
    // used only in MQTT3 where resends are done on timeout of ACKs.
    private final DelayQueue<InFlightPacket> inflightTimeouts = new DelayQueue<>();
    private final Map<Integer, MqttPublishMessage> qos2Receiving = new HashMap<>();
    private ISessionsRepository.SessionData data;
    private boolean resendingNonAcked = false;

    Session(ISessionsRepository.SessionData data, boolean clean, SessionMessageQueue<SessionRegistry.EnqueuedMessage> sessionQueue) {
        if (sessionQueue == null) {
            throw new IllegalArgumentException("sessionQueue parameter can't be null");
        }
        this.data = data;
        this.clean = clean;
        this.sessionQueue = sessionQueue;
        // in MQTT3 cleanSession = true means  expiryInterval=0 else infinite
//        expiryInterval = clean ? 0 : 0xFFFFFFFF;
        this.resendInflightOnTimeout = data.protocolVersion() != MqttVersion.MQTT_5;
    }

    public boolean expireImmediately() {
        return data.expiryInterval() == 0;
    }

    public void updateSessionData(ISessionsRepository.SessionData newSessionData) {
        this.data = newSessionData;
    }

    void markAsNotClean() {
        this.clean = false;
    }

    void markConnecting() {
        assignState(SessionStatus.DISCONNECTED, SessionStatus.CONNECTING);
    }

    boolean completeConnection() {
        return assignState(Session.SessionStatus.CONNECTING, Session.SessionStatus.CONNECTED);
    }

    void bind(MQTTConnection mqttConnection) {
        this.mqttConnection = mqttConnection;
    }

    boolean isBoundTo(MQTTConnection mqttConnection) {
        return this.mqttConnection == mqttConnection;
    }

    public boolean disconnected() {
        return status.get() == SessionStatus.DISCONNECTED;
    }

    public boolean connected() {
        return status.get() == SessionStatus.CONNECTED;
    }

    public String getClientID() {
        return data.clientId();
    }

    public List<Subscription> getSubscriptions() {
        return new ArrayList<>(subscriptions);
    }

    public void addSubscriptions(List<Subscription> newSubscriptions) {
        subscriptions.addAll(newSubscriptions);
    }

    public void removeSubscription(Topic topic) {
        subscriptions.remove(new Subscription(data.clientId(), topic, MqttSubscriptionOption.onlyFromQos(MqttQoS.EXACTLY_ONCE)));
    }

    public boolean hasWill() {
        return getSessionData().hasWill();
    }

    public ISessionsRepository.Will getWill() {
        return getSessionData().will();
    }

    boolean assignState(SessionStatus expected, SessionStatus newState) {
        return status.compareAndSet(expected, newState);
    }

    public void closeImmediately() {
        mqttConnection.dropConnection();
        mqttConnection = null;
        status.set(SessionStatus.DISCONNECTED);
    }

    public void disconnect() {
        final boolean res = assignState(SessionStatus.CONNECTED, SessionStatus.DISCONNECTING);
        if (!res) {
            // someone already moved away from CONNECTED
            // TODO what to do?
            return;
        }

        mqttConnection = null;
        updateSessionData(data.withoutWill());

        assignState(SessionStatus.DISCONNECTING, SessionStatus.DISCONNECTED);
    }

    boolean isClean() {
        return clean;
    }

    public void processPubRec(int pubRecPacketId) {
        // Message discarded, make sure any buffers in it are released
        cleanFromInflight(pubRecPacketId);
        SessionRegistry.EnqueuedMessage removed = inflightWindow.remove(pubRecPacketId);
        if (removed == null) {
            LOG.warn("Received a PUBREC with not matching packetId");
            return;
        }
        Utils.release(removed, "target session - phase 1 Qos2 pull from inflight");
        if (removed instanceof SessionRegistry.PubRelMarker) {
            LOG.info("Received a PUBREC for packetId that was already moved in second step of Qos2");
            return;
        }

        if (mqttConnection == null) {
            return;
        }
        inflightWindow.put(pubRecPacketId, new SessionRegistry.PubRelMarker());
        if (resendInflightOnTimeout) {
            inflightTimeouts.add(new InFlightPacket(pubRecPacketId, FLIGHT_BEFORE_RESEND_MS));
        }
        MqttMessage pubRel = MQTTConnection.pubrel(pubRecPacketId);
        mqttConnection.sendIfWritableElseDrop(pubRel);

        if (resendingNonAcked) {
            resendInflightNotAcked();
        } else {
            drainQueueToConnection();
        }
    }

    public void processPubComp(int messageID) {
        // Message discarded, make sure any buffers in it are released
        cleanFromInflight(messageID);
        SessionRegistry.EnqueuedMessage removed = inflightWindow.remove(messageID);
        if (removed == null) {
            LOG.warn("Received a PUBCOMP with not matching packetId in the inflight cache");
            return;
        }
        Utils.release(removed, "target session - phase 2 Qos2 pull from inflight");
        mqttConnection.sendQuota().releaseSlot();
        drainQueueToConnection();

        // TODO notify the interceptor
//                final InterceptAcknowledgedMessage interceptAckMsg = new InterceptAcknowledgedMessage(inflightMsg,
// topic, username, messageID);
//                m_interceptor.notifyMessageAcknowledged(interceptAckMsg);
    }

    public void sendRetainedPublishOnSessionAtQos(Topic topic, MqttQoS qos, ByteBuf payload,
                                                  MqttProperties.MqttProperty... mqttProperties) {
        final PublishedMessage publishedMessage = new PublishedMessage(topic, qos, payload, true, Instant.MAX, mqttProperties);
        sendPublishOnSessionAtQos(publishedMessage);
    }

    void sendPublishOnSessionAtQos(PublishedMessage publishRequest) {
        switch (publishRequest.getPublishingQos()) {
            case AT_MOST_ONCE:
                if (connected()) {
                    sendPublishQos0(publishRequest);
                }
                break;
            case AT_LEAST_ONCE:
                sendPublishQos1(publishRequest);
                break;
            case EXACTLY_ONCE:
                sendPublishQos2(publishRequest);
                break;
            case FAILURE:
                LOG.error("Not admissible");
        }
    }

    private void sendPublishQos0(PublishedMessage publishRequest) {
        if (publishRequest.isExpired()) {
            LOG.debug("Sending publish at QoS0 already expired, drop it");
            return;
        }

        MqttProperties.MqttProperty[] mqttProperties = publishRequest.updatePublicationExpiryIfPresentOrAdd();
        MqttPublishMessage publishMsg = MQTTConnection.createPublishMessage(publishRequest.getTopic().toString(),
            publishRequest.getPublishingQos(), publishRequest.getPayload(), 0,
            publishRequest.retained, false, mqttProperties);
        mqttConnection.sendPublish(publishMsg);
    }

    private void sendPublishQos1(PublishedMessage publishRequest) {
        if (!connected() && isClean()) {
            //pushing messages to disconnected not clean session
            return;
        }
        if (publishRequest.isExpired()) {
            LOG.debug("Sending publish at QoS1 already expired, expected to happen before {}, drop it", publishRequest.messageExpiry);
            return;
        }

        final MQTTConnection localMqttConnectionRef = mqttConnection;
        sendPublishInFlightWindowOrQueueing(localMqttConnectionRef, publishRequest);
    }

    private void sendPublishQos2(PublishedMessage publishRequest) {
        if (publishRequest.isExpired()) {
            LOG.debug("Sending publish at QoS2 already expired, drop it");
            return;
        }
        final MQTTConnection localMqttConnectionRef = mqttConnection;
        sendPublishInFlightWindowOrQueueing(localMqttConnectionRef, publishRequest);
    }

    private void sendPublishInFlightWindowOrQueueing(MQTTConnection localMqttConnectionRef,
                                                     PublishedMessage publishRequest) {
        // retain the payload because it's going to be added to map or to the queue.
        Utils.retain(publishRequest, "target session - forward to inflight or queue");

        if (canSkipQueue(localMqttConnectionRef)) {
            mqttConnection.sendQuota().consumeSlot();
            int packetId = localMqttConnectionRef.nextPacketId();

            LOG.debug("Adding into inflight for session {} at QoS {}", getClientID(), publishRequest.getPublishingQos());

            EnqueuedMessage old = inflightWindow.put(packetId, publishRequest);
            // If there already was something, release it.
            if (old != null) {
                Utils.release(old, "target session - replace existing slot");
                mqttConnection.sendQuota().releaseSlot();
            }
            if (resendInflightOnTimeout) {
                inflightTimeouts.add(new InFlightPacket(packetId, FLIGHT_BEFORE_RESEND_MS));
            }

            MqttProperties.MqttProperty[] mqttProperties = publishRequest.updatePublicationExpiryIfPresentOrAdd();
            MqttPublishMessage publishMsg = MQTTConnection.createPublishMessage(
                publishRequest.topic.toString(), publishRequest.getPublishingQos(),
                publishRequest.payload, packetId, publishRequest.retained, false, mqttProperties);
            localMqttConnectionRef.sendPublish(publishMsg);

            drainQueueToConnection();
        } else {
            sessionQueue.enqueue(publishRequest);
            LOG.debug("Enqueue to peer session {} at QoS {}", getClientID(), publishRequest.getPublishingQos());
        }
    }

    private boolean canSkipQueue(MQTTConnection localMqttConnectionRef) {
        return localMqttConnectionRef != null &&
            sessionQueue.isEmpty() &&
            mqttConnection.sendQuota().hasFreeSlots() &&
            connected() &&
            localMqttConnectionRef.channel.isWritable();
    }

    private boolean inflightHasSlotsAndConnectionIsUp() {
        return mqttConnection.sendQuota().hasFreeSlots() &&
            connected() &&
            mqttConnection.channel.isWritable();
    }

    void pubAckReceived(int ackPacketId) {
        // TODO remain to invoke in somehow m_interceptor.notifyMessageAcknowledged
        cleanFromInflight(ackPacketId);
        SessionRegistry.EnqueuedMessage removed = inflightWindow.remove(ackPacketId);
        if (removed == null) {
            LOG.warn("Received a PUBACK with not matching packetId({}) in the inflight cache({})",
                ackPacketId, inflightWindow.keySet());
            return;
        }
        Utils.release(removed, "target session - inflight remove");

        mqttConnection.sendQuota().releaseSlot();
        LOG.debug("Received PUBACK {} for session {}", ackPacketId, getClientID());
        if (resendingNonAcked) {
            resendInflightNotAcked();
        } else {
            drainQueueToConnection();
        }
    }

    private void cleanFromInflight(int ackPacketId) {
        inflightTimeouts.removeIf(d -> d.packetId == ackPacketId);
    }

    public void flushAllQueuedMessages() {
        drainQueueToConnection();
    }

    public void resendInflightNotAcked() {
        if (!resendingNonAcked) {
            if (resendInflightOnTimeout) {
                // MQTT3 behavior, resend on timeout
                Collection<InFlightPacket> expired = new ArrayList<>();
                inflightTimeouts.drainTo(expired);
                nonAckPacketIds = expired.stream().map(p -> p.packetId).collect(Collectors.toList());
            } else {
                // MQTT5 behavior resend only not acked present in reopened session.
                // need a copy else removing from the nonAckPacketIds would remove also from inflightWindow
                nonAckPacketIds = new ArrayList<>(inflightWindow.keySet());
            }

            debugLogPacketIds(nonAckPacketIds);
        }

        if (nonAckPacketIds.size() > mqttConnection.sendQuota().availableSlots()) {
            // Send quota is smaller than the inflight messages to resend, split it.
            // Next partition will be sent on PUBREC or PUBACK reception to continue flushing the in flight.
            resendingNonAcked = true;
            List<Integer> partition = nonAckPacketIds.stream()
                .limit(mqttConnection.sendQuota().availableSlots())
                .collect(Collectors.toList());

            resendNonAckedIdsPartition(partition);

            // clean up the partition sent
            for (Integer id : partition) {
                nonAckPacketIds.remove(id);
            }
        } else {
            resendNonAckedIdsPartition(nonAckPacketIds);
            resendingNonAcked = false;
        }

    }

    private void resendNonAckedIdsPartition(Collection<Integer> packetIdsToResend) {
        for (Integer notAckPacketId : packetIdsToResend) {
            final EnqueuedMessage msg = inflightWindow.get(notAckPacketId);
            if (msg == null) {
                // Already acked...
                continue;
            }
            if (msg instanceof SessionRegistry.PubRelMarker) {
                MqttMessage pubRel = MQTTConnection.pubrel(notAckPacketId);
                if (resendInflightOnTimeout) {
                    inflightTimeouts.add(new InFlightPacket(notAckPacketId, FLIGHT_BEFORE_RESEND_MS));
                }
                mqttConnection.sendIfWritableElseDrop(pubRel);
            } else {
                final PublishedMessage pubMsg = (PublishedMessage) msg;
                final Topic topic = pubMsg.topic;
                final MqttQoS qos = pubMsg.publishingQos;
                final ByteBuf payload = pubMsg.payload;
                final MqttProperties.MqttProperty<?>[] mqttProperties = pubMsg.mqttProperties;
                // message fetched from map, but not removed from map. No need to duplicate or release.
                MqttPublishMessage publishMsg = MQTTConnection.createNotRetainedDuplicatedPublishMessage(
                    notAckPacketId, topic, qos, payload, mqttProperties);
                if (resendInflightOnTimeout) {
                    inflightTimeouts.add(new InFlightPacket(notAckPacketId, FLIGHT_BEFORE_RESEND_MS));
                }
                mqttConnection.sendPublish(publishMsg);

                mqttConnection.sendQuota().consumeSlot();
            }
        }
    }

    private void debugLogPacketIds(Collection<Integer> packetIds) {
        if (!LOG.isDebugEnabled() || packetIds.isEmpty()) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (Integer packetId : packetIds) {
            sb.append(packetId).append(", ");
        }
        LOG.debug("Resending {} in flight packets [{}]", packetIds.size(), sb);
    }

    private void drainQueueToConnection() {
        // consume the queue
        while (connected() && !sessionQueue.isEmpty() && inflightHasSlotsAndConnectionIsUp()) {
            final SessionRegistry.EnqueuedMessage msg = sessionQueue.dequeue();
            if (msg == null) {
                // Our message was already fetched by another Thread.
                return;
            }
            final SessionRegistry.PublishedMessage msgPub = (SessionRegistry.PublishedMessage) msg;
            if (msgPub.isExpired()) {
                LOG.debug("Drop an expired message contained in the queue");
                return;
            }

            mqttConnection.sendQuota().consumeSlot();
            int sendPacketId = mqttConnection.nextPacketId();

            // Putting it in a map, but the retain is cancelled out by the below release.
            EnqueuedMessage old = inflightWindow.put(sendPacketId, msg);
            if (old != null) {
                Utils.release(old, "target session - drain queue push to inflight");
                mqttConnection.sendQuota().releaseSlot();
            }
            if (resendInflightOnTimeout) {
                inflightTimeouts.add(new InFlightPacket(sendPacketId, FLIGHT_BEFORE_RESEND_MS));
            }

            MqttProperties.MqttProperty[] mqttProperties = msgPub.updatePublicationExpiryIfPresentOrAdd();

            MqttPublishMessage publishMsg = MQTTConnection.createNotRetainedPublishMessage(
                msgPub.topic.toString(),
                msgPub.publishingQos,
                msgPub.payload,
                sendPacketId,
                mqttProperties);
            mqttConnection.sendPublish(publishMsg);

            // we fetched msg from a map, but the release is cancelled out by the above retain
        }
    }

    public void writabilityChanged() {
        drainQueueToConnection();
    }

    public void reconnectSession() {
        LOG.trace("Republishing all saved messages for session {}", this);
        resendInflightNotAcked();

        if (!resendingNonAcked) {
            // if resend of inflight is bigger than send quota, till it's finished
            // do not drain the queue.
            // send queued messages while offline
            drainQueueToConnection();
        }
    }

    public void receivedPublishQos2(int messageID, MqttPublishMessage msg) {
        // Retain before putting msg in map.
        Utils.retain(msg, "phase 2 qos2");

        MqttPublishMessage old = qos2Receiving.put(messageID, msg);
        // In case of evil client with duplicate msgid.
        Utils.release(old, "phase 2 qos2 - packet id duplicated");

//        mqttConnection.sendPublishReceived(messageID);
    }

    public void receivedPubRelQos2(int messageID) {
        // Done with the message, remove from queue and release payload.
        final MqttPublishMessage removedMsg = qos2Receiving.remove(messageID);
        Utils.release(removedMsg, "phase 2 qos2");
    }

    Optional<InetSocketAddress> remoteAddress() {
        if (connected()) {
            return Optional.of(mqttConnection.remoteAddress());
        }
        return Optional.empty();
    }

    public void cleanUp() {
        // in case of in memory session queues all contained messages
        // has to be released.
        sessionQueue.closeAndPurge();
        inflightTimeouts.clear();
        for (EnqueuedMessage msg : inflightWindow.values()) {
            Utils.release(msg, "session cleanup - inflight window");
        }
        for (MqttPublishMessage msg : qos2Receiving.values()) {
            Utils.release(msg, "session cleanup - phase 2 cache");
        }
    }

    ISessionsRepository.SessionData getSessionData() {
        return this.data;
    }

    /**
     * Disconnect the client from the broker, sending a disconnect and closing the connection
     * */
    public void disconnectFromBroker() {
        mqttConnection.brokerDisconnect(MqttReasonCodes.Disconnect.MALFORMED_PACKET);
        disconnect();
    }

    @Override
    public String toString() {
        return "Session{" +
            "clientId='" + data.clientId() + '\'' +
            ", clean=" + clean +
            ", status=" + status +
            ", inflightSlots=" + mqttConnection.sendQuota().availableSlots() +
            '}';
    }
}

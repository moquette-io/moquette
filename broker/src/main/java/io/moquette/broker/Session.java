package io.moquette.broker;

import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class Session {

    private static final Logger LOG = LoggerFactory.getLogger(Session.class);

    enum SessionStatus {
        CONNECTED, CONNECTING, DISCONNECTING, DISCONNECTED
    }

    static final class Will {

        final String topic;
        final ByteBuf payload;
        final MqttQoS qos;
        final boolean retained;

        Will(String topic, ByteBuf payload, MqttQoS qos, boolean retained) {
            this.topic = topic;
            this.payload = payload;
            this.qos = qos;
            this.retained = retained;
        }
    }

    private final String clientId;
    private boolean clean;
    private Will will;
    private Queue<SessionRegistry.EnqueuedMessage> sessionQueue;
    private final AtomicReference<SessionStatus> status = new AtomicReference<>(SessionStatus.DISCONNECTED);
    private MQTTConnection mqttConnection;
    private List<Subscription> subscriptions = new ArrayList<>();
    private final Map<Integer, SessionRegistry.EnqueuedMessage> inflightWindow = new HashMap<>();
    private final Map<Integer, MqttPublishMessage> qos2Receiving = new HashMap<>();
    private final AtomicInteger inflightSlots = new AtomicInteger(10); // this should be configurable

    Session(String clientId, boolean clean, Will will, Queue<SessionRegistry.EnqueuedMessage> sessionQueue) {
        this.clientId = clientId;
        this.clean = clean;
        this.will = will;
        this.sessionQueue = sessionQueue;
    }

    Session(boolean clean, String clientId, Queue<SessionRegistry.EnqueuedMessage> sessionQueue) {
        this.clientId = clientId;
        this.clean = clean;
        this.sessionQueue = sessionQueue;
    }

    void update(boolean clean, Will will) {
        this.clean = clean;
        this.will = will;
    }

    void markConnected() {
        assignState(SessionStatus.DISCONNECTED, SessionStatus.CONNECTED);
    }

    void bind(MQTTConnection mqttConnection) {
        this.mqttConnection = mqttConnection;
    }

    public boolean disconnected() {
        return status.get() == SessionStatus.DISCONNECTED;
    }

    public boolean connected() {
        return status.get() == SessionStatus.CONNECTED;
    }

    public String getClientID() {
        return clientId;
    }

    public List<Subscription> getSubscriptions() {
        return new ArrayList<>(subscriptions);
    }

    public void addSubscriptions(List<Subscription> newSubscriptions) {
        subscriptions.addAll(newSubscriptions);
    }

    public boolean hasWill() {
        return will != null;
    }

    public Will getWill() {
        return will;
    }

    boolean assignState(SessionStatus expected, SessionStatus newState) {
        return status.compareAndSet(expected, newState);
    }

    public void closeImmediately() {
        mqttConnection.dropConnection();
    }

    public void disconnect() {
        final boolean res = assignState(SessionStatus.CONNECTED, SessionStatus.DISCONNECTING);
        if (!res) {
            // someone already moved away from CONNECTED
            // TODO what to do?
            return;
        }

        mqttConnection = null;
        will = null;

        assignState(SessionStatus.DISCONNECTING, SessionStatus.DISCONNECTED);
    }

    boolean isClean() {
        return clean;
    }

    public void processPubRec(int packetId) {
        inflightWindow.remove(packetId);
        inflightSlots.incrementAndGet();
        if (canSkipQueue()) {
            inflightSlots.decrementAndGet();
            int pubRelPacketId = mqttConnection.nextPacketId();
            inflightWindow.put(pubRelPacketId, new SessionRegistry.PubRelMarker());
            MqttMessage pubRel = MQTTConnection.pubrel(pubRelPacketId);
            mqttConnection.sendIfWritableElseDrop(pubRel);

            drainQueueToConnection();
        } else {
            sessionQueue.add(new SessionRegistry.PubRelMarker());
        }
    }

    public void processPubComp(int messageID) {
        inflightWindow.remove(messageID);
        inflightSlots.incrementAndGet();

        drainQueueToConnection();

        // TODO notify the interceptor
//                final InterceptAcknowledgedMessage interceptAckMsg = new InterceptAcknowledgedMessage(inflightMsg, topic,
//                    username, messageID);
//                m_interceptor.notifyMessageAcknowledged(interceptAckMsg);
    }

    public void sendPublishOnSessionAtQos(Topic topic, MqttQoS qos, ByteBuf payload) {
        switch (qos) {
            case AT_MOST_ONCE:
                if (connected()) {
                    mqttConnection.sendPublishNotRetainedQos0(topic, qos, payload);
                }
                break;
            case AT_LEAST_ONCE:
                sendPublishQos1(topic, qos, payload);
                break;
            case EXACTLY_ONCE:
                sendPublishQos2(topic, qos, payload);
                break;
            case FAILURE:
                LOG.error("Not admissible");
        }
    }

    private void sendPublishQos1(Topic topic, MqttQoS qos, ByteBuf payload) {
        if (!connected() && isClean()) {
            //pushing messages to disconnected not clean session
            return;
        }

        if (canSkipQueue()) {
            inflightSlots.decrementAndGet();
            int packetId = mqttConnection.nextPacketId();
            inflightWindow.put(packetId, new SessionRegistry.PublishedMessage(topic, qos, payload));
            MqttPublishMessage publishMsg = MQTTConnection.notRetainedPublishWithMessageId(topic.toString(), qos, payload, packetId);
            mqttConnection.sendPublish(publishMsg);

            // TODO drainQueueToConnection();?
        } else {
            final SessionRegistry.PublishedMessage msg = new SessionRegistry.PublishedMessage(topic, qos, payload);
            sessionQueue.add(msg);
        }
    }

    private void sendPublishQos2(Topic topic, MqttQoS qos, ByteBuf payload) {
        if (canSkipQueue()) {
            inflightSlots.decrementAndGet();
            int packetId = mqttConnection.nextPacketId();
            inflightWindow.put(packetId, new SessionRegistry.PublishedMessage(topic, qos, payload));
            MqttPublishMessage publishMsg = MQTTConnection.notRetainedPublishWithMessageId(topic.toString(), qos, payload, packetId);
            mqttConnection.sendPublish(publishMsg);

            drainQueueToConnection();
        } else {
            final SessionRegistry.PublishedMessage msg = new SessionRegistry.PublishedMessage(topic, qos, payload);
            sessionQueue.add(msg);
        }
    }

    private boolean canSkipQueue() {
        return sessionQueue.isEmpty() &&
            inflightSlots.get() > 0 &&
            connected() &&
            mqttConnection.channel.isWritable();
    }

    void pubAckReceived(int ackPacketId) {
        inflightWindow.remove(ackPacketId);
        inflightSlots.incrementAndGet();
        drainQueueToConnection();
    }

    private void drainQueueToConnection() {
        // consume the queue
        while (!canSkipQueue()) {
            final SessionRegistry.EnqueuedMessage msg = sessionQueue.remove();
            inflightSlots.decrementAndGet();
            int sendPacketId = mqttConnection.nextPacketId();
            inflightWindow.put(sendPacketId, msg);
            if (msg instanceof SessionRegistry.PubRelMarker) {
                MqttMessage pubRel = MQTTConnection.pubrel(sendPacketId);
                mqttConnection.sendIfWritableElseDrop(pubRel);
            } else {
                final SessionRegistry.PublishedMessage msgPub = (SessionRegistry.PublishedMessage) msg;
                MqttPublishMessage publishMsg = MQTTConnection.notRetainedPublishWithMessageId(msgPub.topic.toString(),
                    msgPub.publishingQos,
                    msgPub.payload, sendPacketId);
                mqttConnection.sendPublish(publishMsg);
            }
        }
    }

    public void writabilityChanged() {
        drainQueueToConnection();
    }

    public void sendQueuedMessagesWhileOffline() {
        LOG.trace("Republishing all saved messages for session {} on CId={}", this, this.clientId);
        drainQueueToConnection();
    }

    void sendRetainedPublishOnSessionAtQos(Topic topic, MqttQoS qos, ByteBuf payload) {
        if (qos != MqttQoS.AT_MOST_ONCE) {
            // QoS 1 or 2
            mqttConnection.sendPublishRetainedWithPacketId(topic, qos, payload);
        } else {
            mqttConnection.sendPublishRetainedQos0(topic, qos, payload);
        }
    }

    public void receivedPublishQos2(int messageID, MqttPublishMessage msg) {
        qos2Receiving.put(messageID, msg);
        msg.retain();
        mqttConnection.sendPublishReceived(messageID);
    }

    public void receivedPubRelQos2(int messageID) {
        qos2Receiving.remove(messageID);
    }

    @Override
    public String toString() {
        return "Session{" +
            "clientId='" + clientId + '\'' +
            ", clean=" + clean +
            ", status=" + status +
            ", inflightSlots=" + inflightSlots +
            '}';
    }
}

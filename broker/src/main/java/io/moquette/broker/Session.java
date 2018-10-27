package io.moquette.broker;

import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class Session {

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
    private final AtomicReference<SessionStatus> status = new AtomicReference<>(SessionStatus.DISCONNECTED);
    private MQTTConnection mqttConnection;
    private List<Subscription> subscriptions = new ArrayList<>();
    private final Map<Integer, MqttPublishMessage> qos2Receiving = new HashMap<>();

    Session(String clientId, boolean clean, Will will) {
        this.clientId = clientId;
        this.clean = clean;
        this.will = will;
    }

    Session(String clientId, boolean clean) {
        this.clientId = clientId;
        this.clean = clean;
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

    void sendRetainedPublish(Topic topic, MqttQoS qos, ByteBuf payload) {
        mqttConnection.sendPublishRetained(topic, qos, payload);
    }

    void sendRetainedPublishWithMessageId(Topic topic, MqttQoS qos, ByteBuf payload) {
        mqttConnection.sendPublishRetainedWithPacketId(topic, qos, payload);
    }

    public void sendPublishOnSessionAtQos(Topic topic, MqttQoS qos, ByteBuf payload) {
        if (qos != MqttQoS.AT_MOST_ONCE) {
            // QoS 1 or 2
            mqttConnection.sendPublishNotRetainedWithMessageId(topic, qos, payload);
        } else {
            mqttConnection.sendPublishNotRetained(topic, qos, payload);
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
}

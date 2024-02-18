package io.moquette.broker;

import io.moquette.broker.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.io.Serializable;
import java.time.Instant;

public class RetainedMessage implements Serializable{

    private final Topic topic;
    private final MqttQoS qos;
    private final byte[] payload;
    private Instant expiryTime;

    public RetainedMessage(Topic topic, MqttQoS qos, byte[] payload) {
        this.topic = topic;
        this.qos = qos;
        this.payload = payload;
    }

    public RetainedMessage(Topic topic, MqttQoS qos, byte[] rawPayload, Instant expiryTime) {
        this(topic, qos, rawPayload);
        this.expiryTime = expiryTime;
    }

    public Topic getTopic() {
        return topic;
    }

    public MqttQoS qosLevel() {
        return qos;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Instant getExpiryTime() {
        return expiryTime;
    }
}

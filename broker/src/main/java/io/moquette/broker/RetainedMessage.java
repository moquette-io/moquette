package io.moquette.broker;

import io.moquette.broker.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.io.Serializable;
import java.time.Instant;

public class RetainedMessage /*implements Serializable*/{

    private final Topic topic;
    private final MqttQoS qos;
    private final byte[] payload;
    private final MqttProperties.MqttProperty[] properties;
    private Instant expiryTime;

    public RetainedMessage(Topic topic, MqttQoS qos, byte[] payload, MqttProperties.MqttProperty[] properties) {
        this.topic = topic;
        this.qos = qos;
        this.payload = payload;
        this.properties = properties;
    }

    public RetainedMessage(Topic topic, MqttQoS qos, byte[] rawPayload, MqttProperties.MqttProperty[] properties, Instant expiryTime) {
        this(topic, qos, rawPayload, properties);
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

    public MqttProperties.MqttProperty[] getMqttProperties() {
        return properties;
    }
}

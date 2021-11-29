package io.moquette.api;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;

public class PublishedMessage extends EnqueuedMessage {

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

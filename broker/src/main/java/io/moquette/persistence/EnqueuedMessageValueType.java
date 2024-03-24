/*
 * Copyright (c) 2012-2021 The original author or authors
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
package io.moquette.persistence;

import io.moquette.broker.SessionRegistry;
import io.moquette.broker.SessionRegistry.EnqueuedMessage;
import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.StringDataType;

import java.nio.ByteBuffer;
import java.time.Instant;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.type.BasicDataType;

public final class EnqueuedMessageValueType extends BasicDataType<EnqueuedMessage> {

    private enum MessageType {PUB_REL_MARKER, PUBLISHED_MESSAGE}

    private final StringDataType topicDataType = new StringDataType();
    private final ByteBufDataType payloadDataType = new ByteBufDataType();
    private final PropertiesDataType propertiesDataType = new PropertiesDataType();

    @Override
    public int compare(EnqueuedMessage a, EnqueuedMessage b) {
        throw DataUtils.newUnsupportedOperationException("Can not compare");
    }

    @Override
    public int getMemory(EnqueuedMessage obj) {
        if (obj instanceof SessionRegistry.PubRelMarker) {
            return 1;
        }
        final SessionRegistry.PublishedMessage casted = (SessionRegistry.PublishedMessage) obj;

        int propertiesSize = hasProperties(casted) ?
            propertiesDataType.getMemory(casted.getMqttProperties()) :
            0;

        return 1 + // message type
            1 + // qos
            topicDataType.getMemory(casted.getTopic().toString()) +
            payloadDataType.getMemory(casted.getPayload()) +
            1 +  // flag to indicate if there are MQttProperties or not
            propertiesSize;
    }

    static boolean hasProperties(SessionRegistry.PublishedMessage casted) {
        return casted.getMqttProperties().length > 0;
    }

    @Override
    public void write(WriteBuffer buff, EnqueuedMessage obj) {
        if (obj instanceof SessionRegistry.PublishedMessage) {
            buff.put((byte) MessageType.PUBLISHED_MESSAGE.ordinal());

            final SessionRegistry.PublishedMessage casted = (SessionRegistry.PublishedMessage) obj;
            buff.put((byte) casted.getPublishingQos().value());

            final String token = casted.getTopic().toString();
            topicDataType.write(buff, token);
            payloadDataType.write(buff, casted.getPayload());
            if (hasProperties(casted)) {
                buff.put((byte) 1); // there are properties
                propertiesDataType.write(buff, casted.getMqttProperties());
            } else {
                buff.put((byte) 0); // there aren't properties
            }
        } else if (obj instanceof SessionRegistry.PubRelMarker) {
            buff.put((byte) MessageType.PUB_REL_MARKER.ordinal());
        } else {
            throw new IllegalArgumentException("Unrecognized message class " + obj.getClass());
        }
    }

    @Override
    public EnqueuedMessage read(ByteBuffer buff) {
        final byte messageType = buff.get();
        if (messageType == MessageType.PUB_REL_MARKER.ordinal()) {
            return new SessionRegistry.PubRelMarker();
        } else if (messageType == MessageType.PUBLISHED_MESSAGE.ordinal()) {
            final MqttQoS qos = MqttQoS.valueOf(buff.get());
            final String topicStr = topicDataType.read(buff);
            final ByteBuf payload = payloadDataType.read(buff);
            if (SerdesUtils.containsProperties(buff)) {
                MqttProperties.MqttProperty[] mqttProperties = propertiesDataType.read(buff);
                return new SessionRegistry.PublishedMessage(Topic.asTopic(topicStr), qos, payload, false, Instant.MAX, mqttProperties);
            } else {
                return new SessionRegistry.PublishedMessage(Topic.asTopic(topicStr), qos, payload, false, Instant.MAX);
            }
        } else {
            throw new IllegalArgumentException("Can't recognize record of type: " + messageType);
        }
    }

    @Override
    public EnqueuedMessage[] createStorage(int i) {
        return new EnqueuedMessage[i];
    }
}

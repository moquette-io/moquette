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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * Utility static methods, like Map get with default value, or elvis operator.
 */
public final class Utils {

    private static final Logger BUFF_LOGGER = LoggerFactory.getLogger("BufferManagement");

    public static void release(Object obj, String tag) {
        if (obj instanceof SessionRegistry.PublishedMessage) {
            SessionRegistry.PublishedMessage msg = (SessionRegistry.PublishedMessage) obj;
            msg.release();
            if (BUFF_LOGGER.isTraceEnabled()) {
                if (tag == null) {
                    BUFF_LOGGER.trace("release object");
                } else {
                    BUFF_LOGGER.trace("release object, tag: {}", tag);
                }
            }
        } else if (obj instanceof ReferenceCounted) {
            ReferenceCountUtil.release(obj);
            if (BUFF_LOGGER.isTraceEnabled()) {
                if (tag == null) {
                    BUFF_LOGGER.trace("release object");
                } else {
                    BUFF_LOGGER.trace("release object, tag: {}", tag);
                }
            }
        }
    }

    public static ByteBufHolder retainDuplicate(ByteBufHolder obj, String tag) {
        final ByteBufHolder duplicate = obj.retainedDuplicate();
        if (BUFF_LOGGER.isTraceEnabled()) {
            if (tag == null) {
                BUFF_LOGGER.trace("retain duplicate");
            } else {
                BUFF_LOGGER.trace("retain duplicate tag: {}", tag);
            }
        }
        return duplicate;
    }

    public static void retain(Object obj, int increment, String tag) {
        if (obj instanceof SessionRegistry.PublishedMessage) {
            SessionRegistry.PublishedMessage msg = (SessionRegistry.PublishedMessage) obj;
            msg.retain();
            if (BUFF_LOGGER.isTraceEnabled()) {
                if (tag == null) {
                    BUFF_LOGGER.trace("retain object incr {}", increment);
                } else {
                    BUFF_LOGGER.trace("retain object incr {}, tag: {}", increment, tag);
                }
            }
        } else if (obj instanceof ReferenceCounted) {
            ReferenceCountUtil.retain(obj, increment);
            if (BUFF_LOGGER.isTraceEnabled()) {
                if (tag == null) {
                    BUFF_LOGGER.trace("retain object incr {}", increment);
                } else {
                    BUFF_LOGGER.trace("retain object incr {}, tag: {}", increment, tag);
                }
            }
        }
    }

    public static void retain(Object obj, String tag) {
        if (obj instanceof SessionRegistry.PublishedMessage) {
            SessionRegistry.PublishedMessage msg = (SessionRegistry.PublishedMessage) obj;
            msg.retain();
            if (BUFF_LOGGER.isTraceEnabled()) {
                if (tag == null) {
                    BUFF_LOGGER.trace("retain object");
                } else {
                    BUFF_LOGGER.trace("retain object, tag: {}", tag);
                }
            }
        } else if (obj instanceof ReferenceCounted) {
            ReferenceCountUtil.retain(obj);
            if (BUFF_LOGGER.isTraceEnabled()) {
                if (tag == null) {
                    BUFF_LOGGER.trace("retain object");
                } else {
                    BUFF_LOGGER.trace("retain object, tag: {}", tag);
                }
            }
        }
    }

    public static <T, K> T defaultGet(Map<K, T> map, K key, T defaultValue) {
        T value = map.get(key);
        if (value != null) {
            return value;
        }
        return defaultValue;
    }

    public static int messageId(MqttMessage msg) {
        return ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
    }

    public static byte[] readBytesAndRewind(ByteBuf payload) {
        byte[] payloadContent = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), payloadContent, 0, payload.readableBytes());
        return payloadContent;
    }

    public static MqttVersion versionFromConnect(MqttConnectMessage msg) {
        return MqttVersion.fromProtocolNameAndLevel(msg.variableHeader().name(), (byte) msg.variableHeader().version());
    }

    private Utils() {
    }

    public static final class Couple<K, L> {
        public final K v1;
        public final L v2;

        public Couple(K v1, L v2) {
            this.v1 = v1;
            this.v2 = v2;
        }

        public static <K, L> Couple<K, L> of(K v1, L v2) {
            return new Couple<>(v1, v2);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Couple<?, ?> couple = (Couple<?, ?>) o;
            return Objects.equals(v1, couple.v1) && Objects.equals(v2, couple.v2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(v1, v2);
        }
    }
}

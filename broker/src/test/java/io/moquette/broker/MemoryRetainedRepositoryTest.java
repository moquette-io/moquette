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

import io.moquette.broker.subscriptions.Topic;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MemoryRetainedRepositoryTest {

    @Test
    public void testRetainedOnTopicReturnsExactTopicMatch() {
        MemoryRetainedRepository repository = new MemoryRetainedRepository();
        Topic retainedTopic = new Topic("foo/bar/baz");
        Topic otherRetainedTopic = new Topic("foo/bar/bazzz");
        
        repository.retain(retainedTopic, MqttMessageBuilders
            .publish()
            .qos(MqttQoS.AT_LEAST_ONCE)
            .topicName("foo/bar/baz")
            .retained(true)
            .payload(Unpooled.buffer(0))
            .build());
        repository.retain(otherRetainedTopic, MqttMessageBuilders
            .publish()
            .qos(MqttQoS.AT_LEAST_ONCE)
            .topicName("foo/bar/bazzz")
            .retained(true)
            .payload(Unpooled.buffer(0))
            .build());

        List<RetainedMessage> retainedMessages = repository.retainedOnTopic("foo/bar/baz");
        
        assertEquals(1, retainedMessages.size());
        assertEquals("foo/bar/baz", retainedMessages.get(0).getTopic().toString());
    }

    @Test
    public void testRetainedOnTopicReturnsWildcardTopicMatch() {
        MemoryRetainedRepository repository = new MemoryRetainedRepository();
        Topic retainedTopic = new Topic("foo/bar/baz");
        Topic otherRetainedTopic = new Topic("foo/baz/bar");

        repository.retain(retainedTopic, MqttMessageBuilders
            .publish()
            .qos(MqttQoS.AT_LEAST_ONCE)
            .topicName("foo/bar/baz")
            .retained(true)
            .payload(Unpooled.buffer(0))
            .build());
        repository.retain(otherRetainedTopic, MqttMessageBuilders
            .publish()
            .qos(MqttQoS.AT_LEAST_ONCE)
            .topicName("foo/baz/bar")
            .retained(true)
            .payload(Unpooled.buffer(0))
            .build());

        List<RetainedMessage> retainedMessages = repository.retainedOnTopic("foo/bar/#");

        assertEquals(1, retainedMessages.size());
        assertEquals("foo/bar/baz", retainedMessages.get(0).getTopic().toString());

        retainedMessages = repository.retainedOnTopic("foo/#");

        assertEquals(2, retainedMessages.size());
        assertEquals("foo/bar/baz", retainedMessages.get(0).getTopic().toString());
        assertEquals("foo/baz/bar", retainedMessages.get(1).getTopic().toString());
    }

    @Test
    public void testRetainedOnTopicReturnsSingleWildcardTopicMatch() {
        MemoryRetainedRepository repository = new MemoryRetainedRepository();
        Topic retainedTopic = new Topic("foo/bar/baz");
        Topic otherRetainedTopic = new Topic("foo/baz/bar");

        repository.retain(retainedTopic, MqttMessageBuilders
            .publish()
            .qos(MqttQoS.AT_LEAST_ONCE)
            .topicName("foo/bar/baz")
            .retained(true)
            .payload(Unpooled.buffer(0))
            .build());
        repository.retain(otherRetainedTopic, MqttMessageBuilders
            .publish()
            .qos(MqttQoS.AT_LEAST_ONCE)
            .topicName("foo/baz/bar")
            .retained(true)
            .payload(Unpooled.buffer(0))
            .build());

        List<RetainedMessage> retainedMessages = repository.retainedOnTopic("foo/+/baz");

        assertEquals(1, retainedMessages.size());
        assertEquals("foo/bar/baz", retainedMessages.get(0).getTopic().toString());
    }
}

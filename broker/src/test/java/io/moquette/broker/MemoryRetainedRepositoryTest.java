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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

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

        Collection<RetainedMessage> retainedMessages = repository.retainedOnTopic("foo/bar/baz");
        
        assertEquals(1, retainedMessages.size());
        assertEquals("foo/bar/baz", retainedMessages.iterator().next().getTopic().toString());
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

        Collection<RetainedMessage> retainedMessages = repository.retainedOnTopic("foo/bar/#");

        assertEquals(1, retainedMessages.size());
        assertEquals("foo/bar/baz", retainedMessages.iterator().next().getTopic().toString());

        retainedMessages = repository.retainedOnTopic("foo/#");

        assertEquals(2, retainedMessages.size());
        Set<String> topicString = retainedMessages.stream()
            .map(RetainedMessage::getTopic)
            .map(Topic::toString)
            .collect(Collectors.toSet());
        assertThat(topicString, containsInAnyOrder("foo/bar/baz", "foo/baz/bar"));
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

        Collection<RetainedMessage> retainedMessages = repository.retainedOnTopic("foo/+/baz");

        assertEquals(1, retainedMessages.size());
        assertEquals("foo/bar/baz", retainedMessages.iterator().next().getTopic().toString());
    }
}

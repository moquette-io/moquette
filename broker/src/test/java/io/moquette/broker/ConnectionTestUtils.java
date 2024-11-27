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

import io.moquette.interception.InterceptHandler;
import io.moquette.interception.BrokerInterceptor;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static org.junit.jupiter.api.Assertions.*;

public final class ConnectionTestUtils {

    public static final List<InterceptHandler> EMPTY_OBSERVERS = Collections.emptyList();
    public static final BrokerInterceptor NO_OBSERVERS_INTERCEPTOR = new BrokerInterceptor(EMPTY_OBSERVERS);

    private ConnectionTestUtils() {
    }

    static void assertConnectAccepted(MQTTConnection connection) {
        assertConnectAccepted((EmbeddedChannel) connection.channel);
    }

    static void assertConnectAccepted(EmbeddedChannel channel) {
        MqttConnAckMessage connAck = channel.readOutbound();
        final MqttConnectReturnCode connAckReturnCode = connAck.variableHeader().connectReturnCode();
        assertEquals(CONNECTION_ACCEPTED, connAckReturnCode, "Connect must be accepted");
    }

    static void verifyReceivePublish(EmbeddedChannel embeddedChannel, String expectedTopic, String expectedContent) {
        MqttPublishMessage receivedPublish = embeddedChannel.flushOutbound().readOutbound();
        assertPublishIsCorrect(expectedTopic, expectedContent, receivedPublish);
        ReferenceCountUtil.release(receivedPublish);
    }

    private static void assertPublishIsCorrect(String expectedTopic, String expectedContent,
                                               MqttPublishMessage receivedPublish) {
        assertNotNull(receivedPublish, "Expecting a PUBLISH message");
        final String decodedPayload = DebugUtils.payload2Str(receivedPublish.payload());
        assertEquals(expectedContent, decodedPayload);
        assertEquals(expectedTopic, receivedPublish.variableHeader().topicName());
    }

//    static void verifyReceiveRetainedPublish(EmbeddedChannel embeddedChannel, String expectedTopic,
//                                             String expectedContent) {
//        MqttPublishMessage receivedPublish = embeddedChannel.readOutbound();
//        assertPublishIsCorrect(expectedTopic, expectedContent, receivedPublish);
//        assertTrue(receivedPublish.fixedHeader().isRetain(),"MUST be retained publish");
//    }

    static void verifyReceiveRetainedPublish(EmbeddedChannel embeddedChannel, String expectedTopic,
                                             String expectedContent, MqttQoS expectedQos) {
        MqttPublishMessage receivedPublish = embeddedChannel.flushOutbound().readOutbound();
        assertEquals(receivedPublish.fixedHeader().qosLevel(), expectedQos);
        assertPublishIsCorrect(expectedTopic, expectedContent, receivedPublish);
        assertTrue(receivedPublish.fixedHeader().isRetain(), "MUST be retained publish");
    }

    static void verifyPublishIsReceived(MQTTConnection connection, MqttQoS expectedQos, String expectedPayload) {
        verifyPublishIsReceived((EmbeddedChannel) connection.channel, expectedQos, expectedPayload);
    }

    static void verifyPublishIsReceived(EmbeddedChannel embCh, MqttQoS expectedQos, String expectedPayload) {
        final MqttPublishMessage publishReceived = embCh.flushOutbound().readOutbound();
        assertNotNull(publishReceived, "A PUB message must be received");
        final String payloadMessage = DebugUtils.payload2Str(publishReceived.payload());
        assertEquals(expectedPayload, payloadMessage, "Sent and received payload must be identical");
        assertEquals(expectedQos, publishReceived.fixedHeader().qosLevel(), "Expected QoS don't match");
    }

    static void verifyNoPublishIsReceived(EmbeddedChannel channel) {
        final Object messageReceived = channel.readOutbound();
        assertNull(messageReceived, "Received an out message from processor while not expected");
    }

    static MqttConnectMessage buildConnect(String clientId) {
        return MqttMessageBuilders.connect()
            .clientId(clientId)
            .build();
    }

    static MqttConnectMessage buildConnectNotClean(String clientId) {
        return MqttMessageBuilders.connect()
            .clientId(clientId)
            .cleanSession(false)
            .build();
    }

    static void connect(MQTTConnection connection, MqttConnectMessage connectMessage) {
        try {
            connection.processConnect(connectMessage).completableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            fail(e);
        }
        assertConnectAccepted((EmbeddedChannel) connection.channel);
    }
}

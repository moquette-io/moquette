/*
 *
 * Copyright (c) 2012-2024 The original author or authors
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
 *
 */

package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubRecException;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PayloadFormatIndicator;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.pubrec.Mqtt5PubRecReasonCode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.codec.mqtt.MqttProperties.BinaryProperty;
import io.netty.handler.codec.mqtt.MqttProperties.IntegerProperty;
import io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType;
import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.shadow.com.univocity.parsers.common.processor.InputValueSwitch;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static org.junit.jupiter.api.Assertions.*;

public class ContentTypeTest extends AbstractServerIntegrationTest {

    // second octet is invalid
    public static final byte[] INVALID_UTF_8_BYTES = new byte[]{(byte) 0xC3, 0x28};

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenAPublishWithContentTypeWhenForwardedToSubscriberThenIsPresent() throws InterruptedException {
        Mqtt5BlockingClient subscriber = createSubscriberClient();
        subscriber.subscribeWith()
            .topicFilter("temperature/living")
            .qos(MqttQos.AT_MOST_ONCE)
            .send();

        Mqtt5BlockingClient publisher = createPublisherClient();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            publisher.publishWith()
                .topic("temperature/living")
                .payload("{\"max\": 18}".getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .qos(MqttQos.AT_MOST_ONCE)
                .send();

            verifyPublishMessage(publishes, msgPub -> {
                assertTrue(msgPub.getContentType().isPresent(), "Content-type MUST be present");
                assertEquals("application/json", msgPub.getContentType().get().toString(), "Content-type MUST be untouched");
            });
        }
    }

    @Test
    public void givenAPublishWithContentTypeRetainedWhenForwardedToSubscriberThenIsPresent() throws InterruptedException, MqttException {
        Mqtt5BlockingClient publisher = createPublisherClient();
        publisher.publishWith()
            .topic("temperature/living")
            .payload("{\"max\": 18}".getBytes(StandardCharsets.UTF_8))
            .retain(true)
            .payloadFormatIndicator(Mqtt5PayloadFormatIndicator.UTF_8)
            .qos(MqttQos.AT_LEAST_ONCE) // retained works for QoS > 0
            .send();

        MqttClient client = new MqttClient("tcp://localhost:1883", "subscriber", new MemoryPersistence());
        client.connect();
        MqttSubscription subscription = new MqttSubscription("temperature/living", 1);
        SubscriptionOptionsTest.PublishCollector publishCollector = new SubscriptionOptionsTest.PublishCollector();
        IMqttToken subscribeToken = client.subscribe(new MqttSubscription[]{subscription},
            new IMqttMessageListener[] {publishCollector});
        TestUtils.verifySubscribedSuccessfully(subscribeToken);

        // Verify the message is also reflected back to the sender
        publishCollector.assertReceivedMessageIn(2, TimeUnit.SECONDS);
        assertEquals("temperature/living", publishCollector.receivedTopic());
        assertEquals("{\"max\": 18}", publishCollector.receivedPayload(), "Payload published on topic should match");
        org.eclipse.paho.mqttv5.common.MqttMessage receivedMessage = publishCollector.receivedMessage();
        assertEquals(MqttQos.AT_LEAST_ONCE.getCode(), receivedMessage.getQos());
        assertTrue(receivedMessage.getProperties().getPayloadFormat());
    }
}

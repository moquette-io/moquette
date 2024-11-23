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
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttProperties.IntegerProperty;
import io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class PayloadFormatIndicatorTest extends AbstractServerIntegrationTest {

    // second octet is invalid
    public static final byte[] INVALID_UTF_8_BYTES = new byte[]{(byte) 0xC3, 0x28};

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenAPublishWithPayloadFormatIndicatorWhenForwardedToSubscriberThenIsPresent() throws InterruptedException {
        Mqtt5BlockingClient subscriber = createSubscriberClient();
        subscriber.subscribeWith()
            .topicFilter("temperature/living")
            .qos(MqttQos.AT_MOST_ONCE)
            .send();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt5BlockingClient publisher = createPublisherClient();
            publisher.publishWith()
                .topic("temperature/living")
                .payload("18".getBytes(StandardCharsets.UTF_8))
                .payloadFormatIndicator(Mqtt5PayloadFormatIndicator.UTF_8)
                .qos(MqttQos.AT_MOST_ONCE)
                .send();

            verifyPublishMessage(publishes, msgPub -> {
                assertTrue(msgPub.getPayloadFormatIndicator().isPresent());
            });
        }
    }

    @Test
    public void givenAPublishWithPayloadFormatIndicatorRetainedWhenForwardedToSubscriberThenIsPresent() throws InterruptedException, MqttException {
        Mqtt5BlockingClient publisher = createPublisherClient();
        publisher.publishWith()
            .topic("temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
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
        assertEquals("18", publishCollector.receivedPayload(), "Payload published on topic should match");
        org.eclipse.paho.mqttv5.common.MqttMessage receivedMessage = publishCollector.receivedMessage();
        assertEquals(MqttQos.AT_LEAST_ONCE.getCode(), receivedMessage.getQos());
        assertTrue(receivedMessage.getProperties().getPayloadFormat());
    }

    @Test
    public void givenNotValidUTF8StringInPublishQoS1WhenPayloadFormatIndicatorIsSetThenShouldReturnBadPublishResponse() {
        Mqtt5BlockingClient subscriber = createSubscriberClient();
        subscriber.subscribeWith()
            .topicFilter("temperature/living")
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();

        Mqtt5BlockingClient publisher = createPublisherClient();
        try {
            publisher.publishWith()
                .topic("temperature/living")
                .payload(INVALID_UTF_8_BYTES)
                .payloadFormatIndicator(Mqtt5PayloadFormatIndicator.UTF_8)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
            fail("Publish with an invalid UTF8 payload and payload format indicator set to UTF8 MUST throw an error");
        } catch (Mqtt5PubAckException pubAckEx) {
            assertEquals(Mqtt5PubAckReasonCode.PAYLOAD_FORMAT_INVALID, pubAckEx.getMqttMessage().getReasonCode());
        }
    }

    @Test
    public void givenNotValidUTF8StringInPublishQoS2WhenPayloadFormatIndicatorIsSetThenShouldReturnBadPublishResponse() {
        Mqtt5BlockingClient subscriber = createSubscriberClient();
        subscriber.subscribeWith()
            .topicFilter("temperature/living")
            .qos(MqttQos.EXACTLY_ONCE)
            .send();

        Mqtt5BlockingClient publisher = createPublisherClient();
        try {
            publisher.publishWith()
                .topic("temperature/living")
                .payload(INVALID_UTF_8_BYTES)
                .payloadFormatIndicator(Mqtt5PayloadFormatIndicator.UTF_8)
                .qos(MqttQos.EXACTLY_ONCE)
                .send();
            fail("Publish with an invalid UTF8 payload and payload format indicator set to UTF8 MUST throw an error");
        } catch (Mqtt5PubRecException pubRecEx) {
            assertEquals(Mqtt5PubRecReasonCode.PAYLOAD_FORMAT_INVALID, pubRecEx.getMqttMessage().getReasonCode());
        }
    }

    @Test
    public void givenNotValidUTF8StringInPublishQoS0WhenPayloadFormatIndicatorIsSetThenShouldReturnDisconnectWithBadPublishResponse() throws InterruptedException {
        connectLowLevel();

        MqttProperties props = new MqttProperties();
        IntegerProperty payloadFormatProperty = new IntegerProperty(MqttPropertyType.PAYLOAD_FORMAT_INDICATOR.value(), 1);
        props.add(payloadFormatProperty);

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, AT_MOST_ONCE,
            false, 0);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("temperature/living", 1, props);
        MqttPublishMessage publishQoS0 = new MqttPublishMessage(fixedHeader, variableHeader, Unpooled.wrappedBuffer(INVALID_UTF_8_BYTES));
        // in a reasonable amount of time (say 500 ms) it should receive a DISCONNECT
        lowLevelClient.publish(publishQoS0);

        // Verify a DISCONNECT is received with PAYLOAD_FORMAT_INVALID reason code and connection is closed
        final MqttMessage receivedMessage = lowLevelClient.receiveNextMessage(Duration.ofMillis(500));
        assertEquals(MqttMessageType.DISCONNECT, receivedMessage.fixedHeader().messageType());
        MqttReasonCodeAndPropertiesVariableHeader disconnectHeader = (MqttReasonCodeAndPropertiesVariableHeader) receivedMessage.variableHeader();
        assertEquals(MqttReasonCodes.Disconnect.PAYLOAD_FORMAT_INVALID.byteValue(), disconnectHeader.reasonCode(),
            "Expected Disconnect to contain PAYLOAD_FORMAT_INVALID as reason code");
        // Ugly hack, but give the Netty channel some instants to receive the disconnection of the socket
        Thread.sleep(100);
        assertTrue(lowLevelClient.isConnectionLost(),"After the disconnect message the connection MUST be closed");
    }
}

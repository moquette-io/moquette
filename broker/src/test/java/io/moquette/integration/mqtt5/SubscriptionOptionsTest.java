/*
 *
 *  * Copyright (c) 2012-2024 The original author or authors
 *  * ------------------------------------------------------
 *  * All rights reserved. This program and the accompanying materials
 *  * are made available under the terms of the Eclipse Public License v1.0
 *  * and Apache License v2.0 which accompanies this distribution.
 *  *
 *  * The Eclipse Public License is available at
 *  * http://www.eclipse.org/legal/epl-v10.html
 *  *
 *  * The Apache License v2.0 is available at
 *  * http://www.opensource.org/licenses/apache2.0.php
 *  *
 *  * You may elect to redistribute this code under either of these licenses.
 *
 */

package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SubscriptionOptionsTest extends AbstractSubscriptionIntegrationTest {

    @Override
    public String clientName() {
        return "client";
    }

    static class PublishCollector implements IMqttMessageListener {
        private final CountDownLatch latch = new CountDownLatch(1);
        private String receivedTopic;
        private MqttMessage receivedMessage;

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            latch.countDown();
            receivedTopic = topic;
            receivedMessage = message;
        }

        public String receivedPayload() {
            return new String(receivedMessage.getPayload(), StandardCharsets.UTF_8);
        }

        public void assertReceivedMessageIn(int time, TimeUnit unit) {
            try {
                assertTrue(latch.await(time, unit), "Publish is received");
            } catch (InterruptedException e) {
                fail("Wait for message was interrupted");
            }
        }

        public void assertNotReceivedMessageIn(int time, TimeUnit unit) {
            try {
                assertFalse(latch.await(time, unit), "Publish MUSTN'T be received");
            } catch (InterruptedException e) {
                fail("Wait for message was interrupted");
            }
        }
    }

    @Test
    public void givenSubscriptionWithNoLocalEnabledWhenTopicMatchPublishByItselfThenNoPublishAreSentBackToSubscriber() throws MqttException {
        MqttClient client = new MqttClient("tcp://localhost:1883", "subscriber", new MemoryPersistence());
        client.connect();
        MqttSubscription subscription = new MqttSubscription("/metering/temp", 1);
        subscription.setNoLocal(true);

        PublishCollector publishCollector = new PublishCollector();
        IMqttToken subscribeToken = client.subscribe(new MqttSubscription[]{subscription},
            new IMqttMessageListener[] {publishCollector});
        verifySubscribedSuccessfully(subscribeToken);

        // publish a message on same topic the client subscribed
        client.publish("/metering/temp", new MqttMessage("18".getBytes(StandardCharsets.UTF_8), 1, false, null));

        // Verify no message is reflected back to the sender
        publishCollector.assertNotReceivedMessageIn(2, TimeUnit.SECONDS);
    }

    @Test
    public void givenSubscriptionWithNoLocalDisabledWhenTopicMatchPublishByItselfThenAPublishAreSentBackToSubscriber() throws MqttException {
        MqttClient client = new MqttClient("tcp://localhost:1883", "subscriber", new MemoryPersistence());
        client.connect();
        MqttSubscription subscription = new MqttSubscription("/metering/temp", 1);
//        subscription.setNoLocal(false);
        PublishCollector publishCollector = new PublishCollector();
        IMqttToken subscribeToken = client.subscribe(new MqttSubscription[]{subscription},
            new IMqttMessageListener[] {publishCollector});
        verifySubscribedSuccessfully(subscribeToken);

        // publish a message on same topic the client subscribed
        client.publish("/metering/temp", new MqttMessage("18".getBytes(StandardCharsets.UTF_8), 1, false, null));

        // Verify the message is also reflected back to the sender
        publishCollector.assertReceivedMessageIn(2, TimeUnit.SECONDS);
        assertEquals("/metering/temp", publishCollector.receivedTopic);
        assertEquals("18", publishCollector.receivedPayload(), "Payload published on topic should match");
        assertEquals(MqttQos.AT_LEAST_ONCE.getCode(), publishCollector.receivedMessage.getQos());
    }

    private static void verifySubscribedSuccessfully(IMqttToken subscribeToken) {
        assertEquals(1, subscribeToken.getReasonCodes().length);
        assertEquals(Mqtt5SubAckReasonCode.GRANTED_QOS_1.getCode(), subscribeToken.getReasonCodes()[0],
            "Client is subscribed to the topic");
    }
}

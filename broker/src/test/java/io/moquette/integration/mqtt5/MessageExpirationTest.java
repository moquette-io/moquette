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

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.awaitility.Awaitility;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MessageExpirationTest extends AbstractServerIntegrationTest {
    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenPublishWithRetainedAndMessageExpiryWhenTimePassedThenRetainedIsNotForwardedOnSubscription() throws InterruptedException, MqttException {
        Mqtt5BlockingClient publisher = createPublisherClient();
        int messageExpiryInterval = 3; //seconds
        publisher.publishWith()
            .topic("temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE) // Broker retains only QoS1 and QoS2
            .retain(true)
            .messageExpiryInterval(messageExpiryInterval)
            .send();

        // let the message expire, wait
        Thread.sleep((messageExpiryInterval + 1) * 1000);

        // subscribe to same topic and verify no message
        Mqtt5BlockingClient subscriber = createSubscriberClient();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            subscriber.subscribeWith()
                .topicFilter("temperature/living")
                .qos(MqttQos.AT_MOST_ONCE)
                .send();

            verifyNoPublish(publishes, v -> {
                }, Duration.ofSeconds(2),
                "Subscriber must not receive any retained message");
        }
    }

    // TODO verify the elapsed
    @Test
    public void givenPublishWithRetainedAndMessageExpiryWhenTimeIsNotExpiredAndSubscriberConnectThenPublishWithRemainingExpiryShouldBeSent() throws Exception {
        Mqtt5BlockingClient publisher = createPublisherClient();
        int messageExpiryInterval = 3; //seconds
        publisher.publishWith()
            .topic("temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE) // Broker retains only QoS1 and QoS2
            .retain(true)
            .messageExpiryInterval(messageExpiryInterval)
            .send();

        Thread.sleep(1_000);

        // subscribe to same topic and verify publish message is received
        connectLowLevel();

        // subscribe with an identifier
        MqttMessage received = lowLevelClient.subscribeWithIdentifier("temperature/living",
            MqttQoS.AT_LEAST_ONCE, 123);
        verifyOfType(received, MqttMessageType.SUBACK);

        // receive a publish message on the subscribed topic
        Awaitility.await()
            .atMost(2, TimeUnit.SECONDS)
            .until(lowLevelClient::hasReceivedMessages);
        MqttMessage mqttMsg = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        verifyOfType(mqttMsg, MqttMessageType.PUBLISH);
        MqttPublishMessage publish = (MqttPublishMessage) mqttMsg;
        MqttProperties.MqttProperty<Integer> messageExpiryProperty = publish.variableHeader()
            .properties()
            .getProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value());
        assertNotNull(messageExpiryProperty, "message expiry property must be present");
        assertTrue(messageExpiryProperty.value() < messageExpiryInterval, "Forwarded message expiry should be lowered");

        // send PUBACK to release
        acknowledge(publish.variableHeader().packetId());
        assertTrue(publish.release(), "Last reference of publish should be released");
    }

    @Test
    public void givenPublishMessageWithExpiryWhenForwarderToSubscriberStillContainsTheMessageExpiryHeader() throws MqttException {
        // Use Paho client to avoid default subscriptionIdentifier (1) set by default by HiveMQ client.
        MqttClient client = new MqttClient("tcp://localhost:1883", "subscriber", new MemoryPersistence());
        client.connect();
        MqttSubscription subscription = new MqttSubscription("temperature/living", 1);
        SubscriptionOptionsTest.PublishCollector publishCollector = new SubscriptionOptionsTest.PublishCollector();
        IMqttToken subscribeToken = client.subscribe(new MqttSubscription[]{subscription},
            new IMqttMessageListener[] {publishCollector});
        TestUtils.verifySubscribedSuccessfully(subscribeToken);

        // publish a message on same topic the client subscribed
        Mqtt5BlockingClient publisher = createPublisherClient();
        long messageExpiryInterval = 3;
        publisher.publishWith()
            .topic("temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE) // Broker retains only QoS1 and QoS2
            .messageExpiryInterval(messageExpiryInterval) // 3 seconds
            .send();

        // Verify the message is also reflected back to the sender
        publishCollector.assertReceivedMessageIn(2, TimeUnit.SECONDS);
        assertEquals("temperature/living", publishCollector.receivedTopic());
        assertEquals("18", publishCollector.receivedPayload(), "Payload published on topic should match");
        org.eclipse.paho.mqttv5.common.MqttMessage receivedMessage = publishCollector.receivedMessage();
        assertEquals(MqttQos.AT_LEAST_ONCE.getCode(), receivedMessage.getQos());
        assertEquals(messageExpiryInterval, receivedMessage.getProperties().getMessageExpiryInterval());
    }

    @Test
    public void givenPublishWithMessageExpiryPropertyWhenItsForwardedToSubscriberThenExpiryValueHasToBeDeducedByTheTimeSpentInBroker() throws InterruptedException {
        int messageExpiryInterval = 10; // seconds
        // avoid the keep alive period could disconnect
        connectLowLevel((int) (messageExpiryInterval * 1.5));

        // subscribe with an identifier
        MqttMessage received = lowLevelClient.subscribeWithIdentifier("temperature/living",
            MqttQoS.AT_LEAST_ONCE, 123, Duration.ofMillis(500));
        verifyOfType(received, MqttMessageType.SUBACK);

        //lowlevel client doesn't ACK any pub, so the in flight window fills up
        Mqtt5BlockingClient publisher = createPublisherClient();
        int inflightWindowSize = 10;
        // fill the in flight window so that messages starts to be enqueued
        fillInFlightWindow(inflightWindowSize, publisher, Integer.MIN_VALUE);

        // send another message, which is enqueued and has an expiry of messageExpiryInterval seconds
        publisher.publishWith()
            .topic("temperature/living")
            .payload(("Enqueued").getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE) // Broker enqueues only QoS1 and QoS2
            .messageExpiryInterval(messageExpiryInterval)
            .send();

        // let time flow so that the message in queue passes its expiry time
        long sleepMillis = Duration.ofSeconds(messageExpiryInterval / 2).toMillis();
        Thread.sleep(sleepMillis);

        // now subscriber consumes messages, shouldn't receive any message in the form "Enqueued-"
        consumesPublishesInflightWindow(inflightWindowSize);
        MqttMessage mqttMessage = lowLevelClient.receiveNextMessage(Duration.ofMillis(1000));

        assertNotNull(mqttMessage, "A publish out of the queue has to be received");
        assertEquals(MqttMessageType.PUBLISH, mqttMessage.fixedHeader().messageType(), "Expected a publish message");
        MqttPublishMessage publishMessage = (MqttPublishMessage) mqttMessage;

        // extract message expiry property
        MqttProperties.MqttProperty expiryProp = publishMessage.variableHeader()
            .properties()
            .getProperty(MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL.value());
        assertNotNull(expiryProp, "Publication expiry property can't be null");
        Integer expirySeconds = ((MqttProperties.IntegerProperty) expiryProp).value();

        assertTrue(expirySeconds < messageExpiryInterval, "Publish's expiry has to be updated");
        assertTrue(publishMessage.release(), "Last reference of publish should be released");
    }

    @Test
    public void givenPublishedMessageWithExpiryWhenMessageRemainInBrokerForMoreThanTheExpiryIsNotPublished() throws InterruptedException {
        int messageExpiryInterval = 2; // seconds
        // avoid the keep alive period could disconnect
        connectLowLevel(messageExpiryInterval * 2);

        // subscribe with an identifier
        MqttMessage received = lowLevelClient.subscribeWithIdentifier("temperature/living",
            MqttQoS.AT_LEAST_ONCE, 123);
        verifyOfType(received, MqttMessageType.SUBACK);

        //lowlevel client doesn't ACK any pub, so the in flight window fills up
        Mqtt5BlockingClient publisher = createPublisherClient();
        int inflightWindowSize = 10;
        // fill the in flight window so that messages starts to be enqueued
        fillInFlightWindow(inflightWindowSize, publisher, messageExpiryInterval);

        // send another message, which is enqueued and has an expiry of messageExpiryInterval seconds
        publisher.publishWith()
            .topic("temperature/living")
            .payload(("Enqueued").getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE) // Broker enqueues only QoS1 and QoS2
            .messageExpiryInterval(messageExpiryInterval)
            .send();

        // let time flow so that the message in queue passes its expiry time
        sleepSeconds(messageExpiryInterval + 1);

        // now subscriber consumes messages, shouldn't receive any message in the form "Enqueued-"
        consumesPublishesInflightWindow(inflightWindowSize);

        MqttMessage mqttMessage = lowLevelClient.receiveNextMessage(Duration.ofMillis(100));
        assertNull(mqttMessage, "No other messages MUST be received after consuming the in flight window");
    }

    private static void fillInFlightWindow(int inflightWindowSize, Mqtt5BlockingClient publisher, int messageExpiryInterval) {
        for (int i = 0; i < inflightWindowSize; i++) {
            Mqtt5PublishBuilder.Send.Complete<Mqtt5PublishResult> builder = publisher.publishWith()
                .topic("temperature/living")
                .payload(Integer.toString(i).getBytes(StandardCharsets.UTF_8))
                .qos(MqttQos.AT_LEAST_ONCE);
            if (messageExpiryInterval != Integer.MIN_VALUE) {
                builder // Broker enqueues only QoS1 and QoS2
                    .messageExpiryInterval(messageExpiryInterval);
            }

            builder.send();
        }
    }
}

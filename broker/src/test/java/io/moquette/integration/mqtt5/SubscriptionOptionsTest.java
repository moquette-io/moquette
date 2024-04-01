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
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5RetainHandling;
import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.jetbrains.annotations.NotNull;
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
        private CountDownLatch latch = new CountDownLatch(1);
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

        public String receivedTopic() {
            return receivedTopic;
        }

        public MqttMessage receivedMessage() {
            return receivedMessage;
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

        public void reset() {
            latch = new CountDownLatch(1);
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
        TestUtils.verifySubscribedSuccessfully(subscribeToken);

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
        TestUtils.verifySubscribedSuccessfully(subscribeToken);

        // publish a message on same topic the client subscribed
        client.publish("/metering/temp", new MqttMessage("18".getBytes(StandardCharsets.UTF_8), 1, false, null));

        // Verify the message is also reflected back to the sender
        publishCollector.assertReceivedMessageIn(2, TimeUnit.SECONDS);
        assertEquals("/metering/temp", publishCollector.receivedTopic);
        assertEquals("18", publishCollector.receivedPayload(), "Payload published on topic should match");
        assertEquals(MqttQos.AT_LEAST_ONCE.getCode(), publishCollector.receivedMessage.getQos());
    }

    @Test
    public void givenAnExistingRetainedMessageWhenClientSubscribeWithAnyRetainAsPublishedSubscriptionOptionThenPublishedMessageIsAlwaysFlaggedAsRetained() throws Exception {
        // publish a retained message, must be at qos => AT_LEAST_ONCE,
        // because AT_MOST_ONCE is not managed in retain (best effort)
        Mqtt5BlockingClient publisher = createPublisherClient();
        publisher.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .retain(true)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();

        MqttClient subscriberWithRetain = new MqttClient("tcp://localhost:1883", "subscriber", new MemoryPersistence());
        subscriberWithRetain.connect();
        MqttSubscription subscription = new MqttSubscription("metric/temperature/living", MqttQos.AT_LEAST_ONCE.getCode());
        subscription.setRetainAsPublished(true);
        subscribeAndVerifyRetainedIsTrue(subscriberWithRetain, subscription);

        MqttClient subscriberWithoutRetain = new MqttClient("tcp://localhost:1883", "subscriber", new MemoryPersistence());
        subscriberWithoutRetain.connect();
        subscription = new MqttSubscription("metric/temperature/living", MqttQos.AT_LEAST_ONCE.getCode());
        subscription.setRetainAsPublished(false);
        subscribeAndVerifyRetainedIsTrue(subscriberWithoutRetain, subscription);
    }

    private static void subscribeAndVerifyRetainedIsTrue(MqttClient subscriberWithRetain, MqttSubscription subscription) throws MqttException {
        PublishCollector publishCollector = new PublishCollector();
        IMqttToken subscribeToken = subscriberWithRetain.subscribe(new MqttSubscription[]{subscription},
            new IMqttMessageListener[] {publishCollector});
        TestUtils.verifySubscribedSuccessfully(subscribeToken);

        // Verify the message is also reflected back to the sender
        publishCollector.assertReceivedMessageIn(2, TimeUnit.SECONDS);
        verifyTopicPayloadAndQoSAsExpected(publishCollector);
        assertTrue(publishCollector.receivedMessage.isRetained());
    }

    private static void verifyTopicPayloadAndQoSAsExpected(PublishCollector publishCollector) {
        assertEquals("metric/temperature/living", publishCollector.receivedTopic);
        assertEquals("18", publishCollector.receivedPayload(), "Payload published on topic should match");
        assertEquals(MqttQos.AT_LEAST_ONCE.getCode(), publishCollector.receivedMessage.getQos());
    }

    @Test
    public void givenSubscriptionWithRetainAsPublishedSetThenRespectTheFlagOnForward() throws MqttException {
        Mqtt5BlockingClient publisher = createPublisherClient();

        PublishCollector publishCollector = new PublishCollector();
        createSubscriberClientWithRetainAsPublished(publishCollector, "metric/temperature/living");

        // publish a retained
        publisher.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .retain(true)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();

        // verify retain flag is respected
        publishCollector.assertReceivedMessageIn(2, TimeUnit.SECONDS);
        verifyTopicPayloadAndQoSAsExpected(publishCollector);
        assertTrue(publishCollector.receivedMessage.isRetained());
        publishCollector.reset();

        // publish a non retained
        publisher.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .retain(false)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();

        // verify retain flag is respected
        publishCollector.assertReceivedMessageIn(2, TimeUnit.SECONDS);
        verifyTopicPayloadAndQoSAsExpected(publishCollector);
        assertFalse(publishCollector.receivedMessage.isRetained());
    }

    @Test
    public void givenSubscriptionWithRetainAsPublishedUnsetThenRetainedFlagIsUnsetOnForwardedPublishes() throws MqttException {
        Mqtt5BlockingClient publisher = createPublisherClient();

        PublishCollector publishCollector = new PublishCollector();
        createSubscriberClientWithoutRetainAsPublished(publishCollector, "metric/temperature/living");

        // publish a retained
        publisher.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .retain(true)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();

        // verify retain flag is respected
        publishCollector.assertReceivedMessageIn(2, TimeUnit.SECONDS);
        verifyTopicPayloadAndQoSAsExpected(publishCollector);
        assertFalse(publishCollector.receivedMessage.isRetained());
        publishCollector.reset();

        // publish a non retained
        publisher.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .retain(false)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();

        // verify retain flag is respected
        publishCollector.assertReceivedMessageIn(2, TimeUnit.SECONDS);
        verifyTopicPayloadAndQoSAsExpected(publishCollector);
        assertFalse(publishCollector.receivedMessage.isRetained());
    }

    @Test
    public void givenFirstSubscriptionWithRetainPolicyToSendAtSubscribeIfNotYetExistsAndARetainedMessagedExistsThenPublishIsReceived() throws Exception {
        Mqtt5BlockingClient publisher = createPublisherClient();
        //publish a retained message
        publisher.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .retain(true)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();

        // receive retained only if new subscription
        PublishCollector publishCollector = new PublishCollector();
        createClientWithRetainPolicy(publishCollector, Mqtt5RetainHandling.SEND_IF_SUBSCRIPTION_DOES_NOT_EXIST.getCode());

        // verify retain flag is respected
        publishCollector.assertReceivedMessageIn(1, TimeUnit.SECONDS);
        verifyTopicPayloadAndQoSAsExpected(publishCollector);
    }

    @Test
    public void givenNonFirstSubscriptionWithRetainPolicyToSendAtSubscribeIfAlreadyExistsAndARetainedMessagedExistsThenPublishIsNotReceived() throws Exception {
        Mqtt5BlockingClient publisher = createPublisherClient();
        //publish a retained message
        publisher.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .retain(true)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();

        // create first subscriber and subscribe to the topic
        final PublishCollector unusedCollector = new PublishCollector();
        createSubscriberClient(unusedCollector, "firstSubscriber");

        // create second subscriber to same topic with RetainPolicy to SendAtSubscribeIfAlreadyExists
        PublishCollector publishCollector = new PublishCollector();
        createClientWithRetainPolicy(publishCollector, Mqtt5RetainHandling.SEND_IF_SUBSCRIPTION_DOES_NOT_EXIST.getCode());

        // verify no retained message is received
        publishCollector.assertNotReceivedMessageIn(2, TimeUnit.SECONDS);
    }

    @Test
    public void givenSubscriptionWithRetainPolicyToDoNotSendAndARetainedMessagedExistsThenPublishIsNotReceived() throws Exception {
        Mqtt5BlockingClient publisher = createPublisherClient();
        //publish a retained message
        publisher.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .retain(true)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();

        // subscriber subscribe to same topic matching the retained but with DO_NOT_SEND policy
        PublishCollector publishCollector = new PublishCollector();
        createClientWithRetainPolicy(publishCollector, Mqtt5RetainHandling.DO_NOT_SEND.getCode());

        // verify no retained message is received
        publishCollector.assertNotReceivedMessageIn(1, TimeUnit.SECONDS);
    }

    private static void createSubscriberClient(PublishCollector publishCollector, String clientId) throws MqttException {
        MqttClient subscriber = new MqttClient("tcp://localhost:1883", clientId, new MemoryPersistence());
        subscriber.connect();
        MqttSubscription subscription = new MqttSubscription("metric/temperature/living", MqttQos.AT_LEAST_ONCE.getCode());

        IMqttToken subscribeToken = subscriber.subscribe(new MqttSubscription[]{subscription},
            new IMqttMessageListener[] {publishCollector});
        TestUtils.verifySubscribedSuccessfully(subscribeToken);
    }

    private static void createClientWithRetainPolicy(PublishCollector publishCollector, int retainPolicy) throws MqttException {
        MqttClient subscriber = new MqttClient("tcp://localhost:1883", "subscriber", new MemoryPersistence());
        subscriber.connect();
        MqttSubscription subscription = new MqttSubscription("metric/temperature/living", MqttQos.AT_LEAST_ONCE.getCode());
        subscription.setRetainHandling(retainPolicy);

        IMqttToken subscribeToken = subscriber.subscribe(new MqttSubscription[]{subscription},
            new IMqttMessageListener[] {publishCollector});
        TestUtils.verifySubscribedSuccessfully(subscribeToken);
    }

    private static MqttClient createSubscriberClientWithRetainAsPublished(PublishCollector publishCollector, String topic) throws MqttException {
        return createSubscriberClient(publishCollector, topic, true);
    }

    private static MqttClient createSubscriberClientWithoutRetainAsPublished(PublishCollector publishCollector, String topic) throws MqttException {
        return createSubscriberClient(publishCollector, topic, false);
    }

    @NotNull
    private static MqttClient createSubscriberClient(PublishCollector publishCollector, String topic, boolean retainAsPublished) throws MqttException {
        MqttClient subscriber = new MqttClient("tcp://localhost:1883", "subscriber", new MemoryPersistence());
        subscriber.connect();
        MqttSubscription subscription = new MqttSubscription(topic, MqttQos.AT_LEAST_ONCE.getCode());
        subscription.setRetainAsPublished(retainAsPublished);

        IMqttToken subscribeToken = subscriber.subscribe(new MqttSubscription[]{subscription},
            new IMqttMessageListener[] {publishCollector});
        TestUtils.verifySubscribedSuccessfully(subscribeToken);

        return subscriber;
    }
}

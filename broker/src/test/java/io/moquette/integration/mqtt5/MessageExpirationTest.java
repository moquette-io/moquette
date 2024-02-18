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
import io.netty.handler.codec.mqtt.*;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.moquette.integration.mqtt5.ConnectTest.verifyNoPublish;
import static org.junit.jupiter.api.Assertions.*;

public class MessageExpirationTest extends AbstractServerIntegrationTest {
    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenPublishWithRetainedAndMessageExpiryWhenTimePassedThenRetainedIsNotForwardedOnSubscription() throws InterruptedException {
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
        subscriber.subscribeWith()
            .topicFilter("temperature/living")
            .qos(MqttQos.AT_MOST_ONCE)
            .send();

        verifyNoPublish(subscriber, v -> {}, Duration.ofSeconds(2),
            "Subscriber must not receive any retained message");
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
    }
}

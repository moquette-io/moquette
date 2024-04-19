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
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PayloadFormatIndicator;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class PayloadFormatIndicatorAndContentTypeTest extends AbstractServerIntegrationTest {
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

        Mqtt5BlockingClient publisher = createPublisherClient();
        publisher.publishWith()
            .topic("temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .payloadFormatIndicator(Mqtt5PayloadFormatIndicator.UTF_8)
            .qos(MqttQos.AT_MOST_ONCE)
            .send();

        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
//            action.accept(null);
            Optional<Mqtt5Publish> publishMessage = publishes.receive(1, TimeUnit.SECONDS);
            if (!publishMessage.isPresent()) {
                fail("Expected to receive a publish message");
                return;
            }
            Mqtt5Publish msgPub = publishMessage.get();
            assertTrue(msgPub.getPayloadFormatIndicator().isPresent());
            final String payload = new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8);
//            assertEquals(expectedPayload, payload, errorMessage);
//            assertEquals(expectedQos, msgPub.getQos());
        }
    }
}

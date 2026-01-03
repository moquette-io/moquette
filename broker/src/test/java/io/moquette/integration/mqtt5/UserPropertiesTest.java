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
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UserPropertiesTest extends AbstractServerIntegrationTest {
    @Override
    public String clientName() {
        return "unused";
    }

    @Test
    public void givenSubscriberWhenPublishWithUserPropertiesMatchingTheTopicFilterArrivesThenUserPropertiesReachTheSubscriber() throws InterruptedException {
        final Mqtt5BlockingClient publisher = createHiveBlockingClient("publisher");
        final Mqtt5BlockingClient subscriber = createHiveBlockingClient("subscriber");
        subscribeToAtQos1(subscriber, "some/interesting/thing");
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {

            Mqtt5PublishResult publishResult = publisher.publishWith()
                .topic("some/interesting/thing")
                .payload("OK".getBytes(StandardCharsets.UTF_8))
                .qos(MqttQos.AT_LEAST_ONCE)
                .userProperties()
                    .add("content-type", "application/plain")
                    .applyUserProperties()
                .send();
            verifyPublishSucceeded(publishResult);

            verifyPublishMessage(publishes, receivedPub -> {
                verifyPayloadInUTF8(receivedPub, "OK");
                verifyContainUserProperty(receivedPub, "content-type", "application/plain");
            });
        }
    }

    @Test
    public void givenRetainedPublishWithUserPropertiesWhenClientSubscribesToMatchingTheTopicFilterThenUserPropertiesReachTheSubscriber() throws InterruptedException {
        final Mqtt5BlockingClient publisher = createHiveBlockingClient("publisher");
        Mqtt5PublishResult publishResult = publisher.publishWith()
            .topic("some/interesting/thing")
            .payload("OK".getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE)
            .retain(true)
            .userProperties()
                .add("content-type", "application/plain")
                .applyUserProperties()
            .send();
        verifyPublishSucceeded(publishResult);

        final Mqtt5BlockingClient subscriber = createHiveBlockingClient("subscriber");
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            subscribeToAtQos1(subscriber, "some/interesting/thing");

            verifyPublishMessage(publishes, receivedPub -> {
                verifyPayloadInUTF8(receivedPub, "OK");
                verifyContainUserProperty(receivedPub, "content-type", "application/plain");
            });
        }
    }

    protected static void verifyContainUserProperty(Mqtt5Publish receivedPub, String expectedName, String expectedValue) {
        Optional<? extends Mqtt5UserProperty> userProp = receivedPub.getUserProperties().asList()
            .stream()
            .filter(prop -> prop.getName().toString().equals(expectedName))
            .findFirst();
        assertTrue(userProp.isPresent(), "Expected a user property named 'content-type'");
        String propertyValue = userProp
            .map(Mqtt5UserProperty::getValue)
            .map(Object::toString)
            .orElse("<empty-string>");
        assertEquals(expectedValue, propertyValue);
    }
}

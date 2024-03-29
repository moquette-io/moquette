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
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.fail;

public class TestUtils {
    static void verifyPublishedMessage(Mqtt5BlockingClient client, Consumer<Mqtt5Publish> verifier, int timeoutSeconds) throws Exception {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Optional<Mqtt5Publish> publishMessage = publishes.receive(timeoutSeconds, TimeUnit.SECONDS);
            if (!publishMessage.isPresent()) {
                fail("Expected to receive a publish message");
                return;
            }
            verifier.accept(publishMessage.get());
        }
    }
}

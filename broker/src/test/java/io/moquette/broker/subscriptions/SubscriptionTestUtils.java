/*
 * Copyright (c) 2012-2023 The original author or authors
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
package io.moquette.broker.subscriptions;

import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import org.jetbrains.annotations.NotNull;

import static io.moquette.broker.subscriptions.Topic.asTopic;

public class SubscriptionTestUtils {
    @NotNull
    static Subscription asSubscription(String clientId, String topicFilter) {
        return new Subscription(clientId, asTopic(topicFilter), MqttSubscriptionOption.onlyFromQos(null));
    }

    @NotNull
    static Subscription asSubscription(String clientId, String topicFilter, String shareName) {
        return new Subscription(clientId, asTopic(topicFilter), MqttSubscriptionOption.onlyFromQos(null), shareName);
    }
}

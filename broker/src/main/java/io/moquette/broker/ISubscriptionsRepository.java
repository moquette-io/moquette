/*
 * Copyright (c) 2012-2018 The original author or authors
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
package io.moquette.broker;

import io.moquette.broker.subscriptions.*;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;

import java.util.Collection;
import java.util.Set;

public interface ISubscriptionsRepository {

    Set<Subscription> listAllSubscriptions();

    void addNewSubscription(Subscription subscription);

    void removeSubscription(String topic, String clientID);

    /**
     * Remove all shared subscription from Storage for a client.
     * */
    void removeAllSharedSubscriptions(String clientId);

    /**
     * Remove shared subscription from Storage.
     * */
    void removeSharedSubscription(String clientId, ShareName share, Topic topicFilter);

    /**
     * Add shared subscription to storage.
     * */
    void addNewSharedSubscription(String clientId, ShareName share, Topic topicFilter, MqttSubscriptionOption option);

    /**
     * Add shared subscription with subscription identifier to storage.
     * */
    void addNewSharedSubscription(String clientId, ShareName share, Topic topicFilter, MqttSubscriptionOption option,
                                  SubscriptionIdentifier subscriptionIdentifier);

    /**
     * List all shared subscriptions to re-add to the tree during a restart.
     * */
    Collection<SharedSubscription> listAllSharedSubscription();
}

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
package io.moquette.persistence;

import io.moquette.broker.ISubscriptionsRepository;
import io.moquette.broker.subscriptions.ShareName;
import io.moquette.broker.subscriptions.SharedSubscription;
import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.SubscriptionIdentifier;
import io.moquette.broker.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class MemorySubscriptionsRepository implements ISubscriptionsRepository {

    private static final Logger LOG = LoggerFactory.getLogger(MemorySubscriptionsRepository.class);
    private final Set<Subscription> subscriptions = new ConcurrentSkipListSet<>();
    private final Map<String, Map<Couple<ShareName, Topic>, SharedSubscription>> sharedSubscriptions = new HashMap<>();

    @Override
    public Set<Subscription> listAllSubscriptions() {
        return Collections.unmodifiableSet(subscriptions);
    }

    @Override
    public void addNewSubscription(Subscription subscription) {
        subscriptions.add(subscription);
    }

    @Override
    public void removeSubscription(String topic, String clientID) {
        subscriptions.stream()
            .filter(s -> s.getTopicFilter().toString().equals(topic) && s.getClientId().equals(clientID))
            .findFirst()
            .ifPresent(subscriptions::remove);
    }

    @Override
    public void removeAllSharedSubscriptions(String clientId) {
        sharedSubscriptions.remove(clientId);
    }

    @Override
    public void removeSharedSubscription(String clientId, ShareName share, Topic topicFilter) {
        Map<Couple<ShareName, Topic>, SharedSubscription> subsMap = sharedSubscriptions.get(clientId);
        if (subsMap == null) {
            LOG.info("Removing a non existing shared subscription for client: {}", clientId);
            return;
        }
        subsMap.remove(Couple.of(share, topicFilter));
        if (subsMap.isEmpty()) {
            // clean up an empty sub map
            sharedSubscriptions.remove(clientId);
        }
    }

    @Override
    public void addNewSharedSubscription(String clientId, ShareName share, Topic topicFilter, MqttQoS requestedQoS) {
        SharedSubscription sharedSub = new SharedSubscription(share, topicFilter, clientId, requestedQoS);
        storeNewSharedSubscription(clientId, share, topicFilter, sharedSub);
    }

    private void storeNewSharedSubscription(String clientId, ShareName share, Topic topicFilter, SharedSubscription sharedSub) {
        Map<Couple<ShareName, Topic>, SharedSubscription> subsMap = sharedSubscriptions.computeIfAbsent(clientId, unused -> new HashMap<>());
        subsMap.put(Couple.of(share, topicFilter), sharedSub);
    }

    @Override
    public void addNewSharedSubscription(String clientId, ShareName share, Topic topicFilter, MqttQoS requestedQoS,
                                         SubscriptionIdentifier subscriptionIdentifier) {
        SharedSubscription sharedSub = new SharedSubscription(share, topicFilter, clientId, requestedQoS, subscriptionIdentifier);
        storeNewSharedSubscription(clientId, share, topicFilter, sharedSub);
    }

    @Override
    public Collection<SharedSubscription> listAllSharedSubscription() {
        final List<SharedSubscription> result = new ArrayList<>();
        for (Map.Entry<String, Map<Couple<ShareName, Topic>, SharedSubscription>> entry : sharedSubscriptions.entrySet()) {
            for (Map.Entry<Couple<ShareName, Topic>, SharedSubscription> nestedEntry : entry.getValue().entrySet()) {
                result.add(nestedEntry.getValue());
            }
        }
        return result;
    }
}

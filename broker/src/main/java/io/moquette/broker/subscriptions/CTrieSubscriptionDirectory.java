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
package io.moquette.broker.subscriptions;

import io.moquette.broker.ISubscriptionsRepository;
import io.moquette.broker.subscriptions.CTrie.SubscriptionRequest;
import io.moquette.broker.subscriptions.CTrie.UnsubscribeRequest;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CTrieSubscriptionDirectory implements ISubscriptionsDirectory {

    private static final Logger LOG = LoggerFactory.getLogger(CTrieSubscriptionDirectory.class);

    private CTrie ctrie;
    private volatile ISubscriptionsRepository subscriptionsRepository;

    private final ConcurrentMap<String, List<SharedSubscription>> clientSharedSubscriptions = new ConcurrentHashMap<>();

    @Override
    public void init(ISubscriptionsRepository subscriptionsRepository) {
        LOG.info("Initializing CTrie");
        ctrie = new CTrie();

        LOG.info("Initializing subscriptions store...");
        this.subscriptionsRepository = subscriptionsRepository;
        // reload any subscriptions persisted
        if (LOG.isTraceEnabled()) {
            LOG.trace("Reloading all stored subscriptions. SubscriptionTree = {}", dumpTree());
        }

        for (Subscription subscription : this.subscriptionsRepository.listAllSubscriptions()) {
            LOG.debug("Re-subscribing {}", subscription);
            ctrie.addToTree(SubscriptionRequest.buildNonShared(subscription));
        }

        for (SharedSubscription shared : subscriptionsRepository.listAllSharedSubscription()) {
            LOG.debug("Re-subscribing shared {}", shared);
            ctrie.addToTree(SubscriptionRequest.buildShared(shared.getShareName(), shared.topicFilter(),
                shared.clientId(), MqttSubscriptionOption.onlyFromQos(shared.requestedQoS())));
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Stored subscriptions have been reloaded. SubscriptionTree = {}", dumpTree());
        }
    }

    /**
     * Given a topic string return the clients subscriptions that matches it. Topic string can't
     * contain character # and + because they are reserved to listeners subscriptions, and not topic
     * publishing.
     *
     * @param topicName
     *            to use for search matching subscriptions.
     * @return the list of matching subscriptions, or empty if not matching.
     */
    @Override
    public List<Subscription> matchWithoutQosSharpening(Topic topicName) {
        return ctrie.recursiveMatch(topicName);
    }

    @Override
    public List<Subscription> matchQosSharpening(Topic topicName) {
        final List<Subscription> subscriptions = matchWithoutQosSharpening(topicName);

        // for each session select the subscription with higher QoS
        return selectSubscriptionsWithHigherQoSForEachSession(subscriptions);
    }

    private static List<Subscription> selectSubscriptionsWithHigherQoSForEachSession(List<Subscription> subscriptions) {
        // for each session select the subscription with higher QoS
        Map<String, Subscription> subsGroupedByClient = new HashMap<>();
        for (Subscription sub : subscriptions) {
            // If same client is subscribed to two different shared subscription that overlaps
            // then it has to return both subscriptions, because the share name made them independent.
            final String key = sub.clientAndShareName();
            Subscription existingSub = subsGroupedByClient.get(key);
            // update the selected subscriptions if not present or if it has a greater qos
            if (existingSub == null || existingSub.qosLessThan(sub)) {
                subsGroupedByClient.put(key, sub);
            }
        }
        return new ArrayList<>(subsGroupedByClient.values());
    }

    @Override
    public boolean add(String clientId, Topic filter, MqttSubscriptionOption option) {
        SubscriptionRequest subRequest = SubscriptionRequest.buildNonShared(clientId, filter, option);
        return addNonSharedSubscriptionRequest(subRequest);
    }

    @Override
    public boolean add(String clientId, Topic filter, MqttSubscriptionOption option, SubscriptionIdentifier subscriptionId) {
        SubscriptionRequest subRequest = SubscriptionRequest.buildNonShared(clientId, filter, option, subscriptionId);
        return addNonSharedSubscriptionRequest(subRequest);
    }

    private boolean addNonSharedSubscriptionRequest(SubscriptionRequest subRequest) {
        boolean notExistingSubscription = ctrie.addToTree(subRequest);
        subscriptionsRepository.addNewSubscription(subRequest.subscription());
        return notExistingSubscription;
    }

    @Override
    public void addShared(String clientId, ShareName name, Topic topicFilter, MqttSubscriptionOption option) {
        SubscriptionRequest shareSubRequest = SubscriptionRequest.buildShared(name, topicFilter, clientId, option);
        addSharedSubscriptionRequest(shareSubRequest);
    }

    private void addSharedSubscriptionRequest(SubscriptionRequest shareSubRequest) {
        ctrie.addToTree(shareSubRequest);

        if (shareSubRequest.hasSubscriptionIdentifier()) {
            subscriptionsRepository.addNewSharedSubscription(shareSubRequest.getClientId(), shareSubRequest.getSharedName(),
                shareSubRequest.getTopicFilter(), shareSubRequest.getOption(), shareSubRequest.getSubscriptionIdentifier());
        } else {
            subscriptionsRepository.addNewSharedSubscription(shareSubRequest.getClientId(), shareSubRequest.getSharedName(),
                shareSubRequest.getTopicFilter(), shareSubRequest.getOption());
        }
        List<SharedSubscription> sharedSubscriptions = clientSharedSubscriptions.computeIfAbsent(shareSubRequest.getClientId(), unused -> new ArrayList<>());
        sharedSubscriptions.add(shareSubRequest.sharedSubscription());
    }

    @Override
    public void addShared(String clientId, ShareName name, Topic topicFilter, MqttSubscriptionOption option,
                          SubscriptionIdentifier subscriptionId) {
        SubscriptionRequest shareSubRequest = SubscriptionRequest.buildShared(name, topicFilter, clientId, option, subscriptionId);
        addSharedSubscriptionRequest(shareSubRequest);
    }

    /**
     * Removes subscription from CTrie, adds TNode when the last client unsubscribes, then calls for cleanTomb in a
     * separate atomic CAS operation.
     *
     * @param topic the subscription's topic to remove.
     * @param clientID the Id of client owning the subscription.
     */
    @Override
    public void removeSubscription(Topic topic, String clientID) {
        UnsubscribeRequest request = UnsubscribeRequest.buildNonShared(clientID, topic);
        ctrie.removeFromTree(request);
        this.subscriptionsRepository.removeSubscription(topic.toString(), clientID);
    }

    @Override
    public void removeSharedSubscription(ShareName name, Topic topicFilter, String clientId) {
        UnsubscribeRequest request = UnsubscribeRequest.buildShared(name, topicFilter, clientId);
        ctrie.removeFromTree(request);

        subscriptionsRepository.removeSharedSubscription(clientId, name, topicFilter);

        SharedSubscription sharedSubscription = new SharedSubscription(name, topicFilter, clientId, MqttSubscriptionOption.onlyFromQos(MqttQoS.AT_MOST_ONCE) /* UNUSED in compare */);
        List<SharedSubscription> sharedSubscriptions = clientSharedSubscriptions.get(clientId);
        if (sharedSubscriptions != null && !sharedSubscriptions.isEmpty()) {
            sharedSubscriptions.remove(sharedSubscription);
            clientSharedSubscriptions.replace(clientId, sharedSubscriptions);
        }
    }

    @Override
    public int size() {
        return ctrie.size();
    }

    @Override
    public String dumpTree() {
        return ctrie.dumpTree();
    }

    @Override
    public void removeSharedSubscriptionsForClient(String clientId) {
        List<SharedSubscription> sessionSharedSubscriptions = clientSharedSubscriptions.remove(clientId);
        if (sessionSharedSubscriptions != null) {
            // remove the client from all shared subscriptions
            for (SharedSubscription subscription : sessionSharedSubscriptions) {
                UnsubscribeRequest request = UnsubscribeRequest.buildShared(subscription.getShareName(), subscription.topicFilter(), clientId);
                ctrie.removeFromTree(request);
            }
        }

        subscriptionsRepository.removeAllSharedSubscriptions(clientId);
    }
}

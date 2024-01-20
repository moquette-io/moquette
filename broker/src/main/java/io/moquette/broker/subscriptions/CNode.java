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

import io.moquette.broker.subscriptions.CTrie.SubscriptionRequest;
import io.moquette.broker.subscriptions.CTrie.UnsubscribeRequest;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

class CNode implements Comparable<CNode> {

    public static final Random SECURE_RANDOM = new SecureRandom();
    private final Token token;
    private final List<INode> children;
    // Sorted list of subscriptions. The sort is necessary for fast access, instead of linear scan.
    private List<Subscription> subscriptions;
    // the list of SharedSubscription is sorted. The sort is necessary for fast access, instead of linear scan.
    private Map<ShareName, List<SharedSubscription>> sharedSubscriptions;

    CNode(Token token) {
        this.children = new ArrayList<>();
        this.subscriptions = new ArrayList<>();
        this.sharedSubscriptions = new HashMap<>();
        this.token = token;
    }

    //Copy constructor
    private CNode(Token token, List<INode> children, List<Subscription> subscriptions, Map<ShareName,
                  List<SharedSubscription>> sharedSubscriptions) {
        this.token = token; // keep reference, root comparison in directory logic relies on it for now.
        this.subscriptions = new ArrayList<>(subscriptions);
        this.sharedSubscriptions = new HashMap<>(sharedSubscriptions);
        this.children = new ArrayList<>(children);
    }

    public Token getToken() {
        return token;
    }

    List<INode> allChildren() {
        return new ArrayList<>(this.children);
    }

    Optional<INode> childOf(Token token) {
        int idx = findIndexForToken(token);
        if (idx < 0) {
            return Optional.empty();
        }
        return Optional.of(children.get(idx));
    }

    private int findIndexForToken(Token token) {
        final INode tempTokenNode = new INode(new CNode(token));
        return Collections.binarySearch(children, tempTokenNode, (INode node, INode tokenHolder) -> node.mainNode().token.compareTo(tokenHolder.mainNode().token));
    }

    @Override
    public int hashCode() {
        return Objects.hash(token);
    }

    CNode copy() {
        return new CNode(this.token, this.children, this.subscriptions, this.sharedSubscriptions);
    }

    public void add(INode newINode) {
        int idx = findIndexForToken(newINode.mainNode().token);
        if (idx < 0) {
            children.add(-1 - idx, newINode);
        } else {
            children.add(idx, newINode);
        }
    }

    public void remove(INode node) {
        int idx = findIndexForToken(node.mainNode().token);
        this.children.remove(idx);
    }

    private List<Subscription> sharedSubscriptions() {
        List<Subscription> selectedSubscriptions = new ArrayList<>(sharedSubscriptions.size());
        // for each sharedSubscription related to a ShareName, select one subscription
        for (Map.Entry<ShareName, List<SharedSubscription>> subsForName : sharedSubscriptions.entrySet()) {
            List<SharedSubscription> list = subsForName.getValue();
            final String shareName = subsForName.getKey().getShareName();
            // select a subscription randomly
            int randIdx = SECURE_RANDOM.nextInt(list.size());
            SharedSubscription sub = list.get(randIdx);
            selectedSubscriptions.add(sub.createSubscription());
        }
        return selectedSubscriptions;
    }

    List<Subscription> subscriptions() {
        return subscriptions;
    }

    // Mutating operation
    CNode addSubscription(SubscriptionRequest request) {
        if (request.isShared()) {
            final ShareName shareName = request.getSharedName();
            final SharedSubscription newSubscription = request.sharedSubscription();
            List<SharedSubscription> subscriptions = sharedSubscriptions.getOrDefault(shareName, new ArrayList<>());
            // if a shared subscription already exists for same clientId and share name, overwrite, because
            // the client could desire to update it.
            int idx = Collections.binarySearch(subscriptions, newSubscription);
            if (idx >= 0) {
                subscriptions.set(idx, newSubscription);
            } else {
                subscriptions.add(-1 - idx, newSubscription);
            }
            sharedSubscriptions.put(shareName, subscriptions);
        } else {
            final Subscription newSubscription = request.subscription();

            // if already contains one with same topic and same client, keep that with higher QoS
            int idx = Collections.binarySearch(subscriptions, newSubscription);
            if (idx >= 0) {
                // Subscription already exists
                final Subscription existing = subscriptions.get(idx);
                if (needsToUpdateExistingSubscription(newSubscription, existing)) {
                    subscriptions.set(idx, newSubscription);
                }
            } else {
                // insert into the expected index so that the sorting is maintained
                this.subscriptions.add(-1 - idx, newSubscription);
            }
        }
        return this;
    }

    private static boolean needsToUpdateExistingSubscription(Subscription newSubscription, Subscription existing) {
        if ((newSubscription.hasSubscriptionIdentifier() && existing.hasSubscriptionIdentifier()) &&
            newSubscription.getSubscriptionIdentifier().equals(existing.getSubscriptionIdentifier())
        ) {
            // if subscription identifier hasn't changed,
            // then check QoS but don't lower the requested QoS level
            return existing.option().qos().value() < newSubscription.option().qos().value();
        }

        // subscription identifier changed
        // TODO need to understand if requestedQoS has to be also replaced or not, if not
        // the existing QoS has to be copied. This to avoid that a subscription identifier
        // change silently break the rule of existing qos never lowered.
        return true;
    }

    /**
     * @return true iff the subscriptions contained in this node are owned by clientId
     *   AND at least one subscription is actually present for that clientId
     * */
    boolean containsOnly(String clientId) {
        for (Subscription sub : this.subscriptions) {
            if (!sub.clientId.equals(clientId)) {
                return false;
            }
        }
        return !this.subscriptions.isEmpty();
    }

    public boolean contains(String clientId) {
        return containsSubscriptionsForClient(clientId) || containsSharedSubscriptionsForClient(clientId);
    }

    private boolean containsSharedSubscriptionsForClient(String clientId) {
        boolean result = false;
        for (List<SharedSubscription> sharedForShareName : this.sharedSubscriptions.values()) {
            SharedSubscription keyWrapper = wrapKey(clientId);
            Comparator<SharedSubscription> compareByClientId = Comparator.comparing(SharedSubscription::clientId);
            int res = Collections.binarySearch(sharedForShareName, keyWrapper, compareByClientId);
            result = res >= 0 || result;
        }
        return result;
    }

    private static SharedSubscription wrapKey(String clientId) {
        MqttQoS unusedQoS = MqttQoS.AT_MOST_ONCE;
        return new SharedSubscription(null, Topic.asTopic("unused"), clientId, MqttSubscriptionOption.onlyFromQos(unusedQoS));
    }

    //TODO this is equivalent to negate(containsOnly(clientId))
    private boolean containsSubscriptionsForClient(String clientId) {
        for (Subscription sub : this.subscriptions) {
            if (sub.clientId.equals(clientId)) {
                return true;
            }
        }
        return false;
    }

    void removeSubscriptionsFor(UnsubscribeRequest request) {
        String clientId = request.getClientId();

        if (request.isShared()) {
            List<SharedSubscription> subscriptionsForName = this.sharedSubscriptions.get(request.getSharedName());
            List<SharedSubscription> toRemove = subscriptionsForName.stream()
                .filter(sub -> sub.clientId().equals(clientId))
                .collect(Collectors.toList());
            subscriptionsForName.removeAll(toRemove);

            if (subscriptionsForName.isEmpty()) {
                this.sharedSubscriptions.remove(request.getSharedName());
            } else {
                this.sharedSubscriptions.replace(request.getSharedName(), subscriptionsForName);
            }
        } else {
            // collect Subscription instances to remove
            Set<Subscription> toRemove = new HashSet<>();
            for (Subscription sub : this.subscriptions) {
                if (sub.clientId.equals(clientId)) {
                    toRemove.add(sub);
                }
            }
            // effectively remove the instances
            this.subscriptions.removeAll(toRemove);
        }
    }

    @Override
    public int compareTo(CNode o) {
        return token.compareTo(o.token);
    }

    public List<Subscription> sharedAndNonSharedSubscriptions() {
        List<Subscription> shared = sharedSubscriptions();
        List<Subscription> returnedSubscriptions = new ArrayList<>(subscriptions.size() + shared.size());
        returnedSubscriptions.addAll(subscriptions);
        returnedSubscriptions.addAll(shared);
        return returnedSubscriptions;
    }
}

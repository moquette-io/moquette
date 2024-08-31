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
import java.util.Collection;
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
import org.pcollections.PMap;
import org.pcollections.TreePMap;

class CNode implements Comparable<CNode> {

    public static final Random SECURE_RANDOM = new SecureRandom();
    private final Token token;
    private PMap<String, INode> children;
    // Map of subscriptions per clientId.
    private PMap<String, Subscription> subscriptions;
    // the list of SharedSubscription is sorted. The sort is necessary for fast access, instead of linear scan.
    private PMap<ShareName, List<SharedSubscription>> sharedSubscriptions;

    CNode(Token token) {
        this.children = TreePMap.empty();
        this.subscriptions = TreePMap.empty();
        this.sharedSubscriptions = TreePMap.empty();
        this.token = token;
    }

    //Copy constructor
    private CNode(Token token, PMap<String, INode> children, PMap<String, Subscription> subscriptions, PMap<ShareName, List<SharedSubscription>> sharedSubscriptions) {
        this.token = token; // keep reference, root comparison in directory logic relies on it for now.
        this.subscriptions = subscriptions;
        this.sharedSubscriptions = sharedSubscriptions;
        this.children = children;
    }

    public Token getToken() {
        return token;
    }

    Collection<INode> allChildren() {
        return this.children.values();
    }

    Optional<INode> childOf(Token token) {
        INode value = children.get(token.name);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token);
    }

    CNode copy() {
        return new CNode(this.token, this.children, this.subscriptions, this.sharedSubscriptions);
    }

    public void add(INode newINode) {
        final String tokenName = newINode.mainNode().token.name;
        children = children.plus(tokenName, newINode);
    }

    public INode remove(INode node) {
        final String tokenName = node.mainNode().token.name;
        INode toRemove = children.get(tokenName);
        children = children.minus(tokenName);
        return toRemove;
    }

    public PMap<String, Subscription> getSubscriptions() {
        return subscriptions;
    }

    public PMap<ShareName, List<SharedSubscription>> getSharedSubscriptions() {
        return sharedSubscriptions;
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
            final Subscription existing = subscriptions.get(newSubscription.clientId);
            if (existing != null) {
                // Subscription already exists
                if (needsToUpdateExistingSubscription(newSubscription, existing)) {
                    subscriptions = subscriptions.plus(newSubscription.clientId, newSubscription);
                }
            } else {
                // insert into the expected index so that the sorting is maintained
                subscriptions = subscriptions.plus(newSubscription.clientId, newSubscription);
            }
        }
        return this;
    }

    private static boolean needsToUpdateExistingSubscription(Subscription newSubscription, Subscription existing) {
        if ((newSubscription.hasSubscriptionIdentifier() && existing.hasSubscriptionIdentifier())
            && newSubscription.getSubscriptionIdentifier().equals(existing.getSubscriptionIdentifier())) {
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
        for (String sub : this.subscriptions.keySet()) {
            if (!sub.equals(clientId)) {
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
        return subscriptions.containsKey(clientId);
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
                sharedSubscriptions = sharedSubscriptions.minus(request.getSharedName());
            } else {
                sharedSubscriptions = sharedSubscriptions.plus(request.getSharedName(), subscriptionsForName);
            }
        } else {
            subscriptions = subscriptions.minus(clientId);
        }
    }

    @Override
    public int compareTo(CNode o) {
        return token.compareTo(o.token);
    }

}

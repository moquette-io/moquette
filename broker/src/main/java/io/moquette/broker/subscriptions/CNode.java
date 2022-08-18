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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class CNode {

    private Token token;
    private final Map<Token, CNode> children;
    private final Map<Subscription, Subscription> subscriptions;

    CNode() {
        this.children = new ConcurrentHashMap<>();
        this.subscriptions = new ConcurrentHashMap<>();
    }

    public Token getToken() {
        return token;
    }

    public void setToken(Token token) {
        this.token = token;
    }

    @Override
    public int hashCode() {
        return Objects.hash(token);
    }

    public boolean childrenIsEmpty() {
        return children.isEmpty();
    }

    Map<Token, CNode> allChildren() {
        return this.children;
    }

    CNode getChild(Token token) {
        return this.children.get(token);
    }

    CNode computeChildIfAbsent(Token token) {
        synchronized (this.children) {
            return this.children.computeIfAbsent(token, token1 -> {
                CNode cNode = new CNode();
                cNode.setToken(token1);
                return cNode;
            });
        }
    }

    CNode removeEmptyChild(Token token) {
        synchronized (this.children) {
            CNode child = children.get(token);
            if (child.subscriptionIsEmpty() && child.childrenIsEmpty()) {
                return this.children.remove(token);
            }
            return null;
        }
    }

    public boolean subscriptionIsEmpty() {
        return subscriptions.isEmpty();
    }

    public int subscriptionSize() {
        return subscriptions.size();
    }

    public Set<Subscription> allSubscription() {
        return subscriptions.keySet();
    }

    void addSubscription(Subscription newSubscription) {
        Subscription existing = subscriptions.get(newSubscription);
        // if already contains one with same topic and same client, keep that with higher QoS
        if (existing != null && existing.getRequestedQos().value() < newSubscription.getRequestedQos().value()) {
            Subscription remove = this.subscriptions.remove(newSubscription);
            if (remove == null || remove.getRequestedQos().value() > newSubscription.getRequestedQos().value()) {
                return;
            }
        }
        this.subscriptions.put(newSubscription, newSubscription);
    }

    void removeSubscription(Subscription subscription) {
        this.subscriptions.remove(subscription);
    }

}

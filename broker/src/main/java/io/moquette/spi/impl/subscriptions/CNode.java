/*
 * Copyright (c) 2012-2017 The original author or authors
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
package io.moquette.spi.impl.subscriptions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

class CNode {

    Token token;
    private List<INode> children;
    Set<Subscription> subscriptions;

    //private int subtreeSubscriptions;

    CNode() {
        ConcurrentHashMap<Subscription, Object> cmap = new ConcurrentHashMap<Subscription, Object>();
        subscriptions = cmap.newKeySet();
        children = new CopyOnWriteArrayList<>();
    }

    //Copy constructor
    private CNode(Token token, List<INode> children, Set<Subscription> subscriptions) {
        ConcurrentHashMap<Subscription, Integer> cmap = new ConcurrentHashMap<Subscription, Integer>();
        this.subscriptions = cmap.newKeySet();
        this.subscriptions.addAll(subscriptions);

        this.token = token;
        this.children = new CopyOnWriteArrayList<>(children);
    }

    boolean anyChildrenMatch(Token token) {
        for (INode iNode : children) {
            final CNode child = iNode.mainNode();
            if (child.equals(token)) {
                return true;
            }
        }
        return false;
    }

    List<INode> allChildren() {
        return this.children;
    }

    INode childOf(Token token) {
        for (INode iNode : children) {
            final CNode child = iNode.mainNode();
            if (child.equals(token)) {
                return iNode;
            }
        }
        throw new IllegalArgumentException("Asked for a token that doesn't exists in any child [" + token + "]");
    }

    private boolean equals(Token token) {
        return this.token != null && this.token.equals(token);
    }

    CNode copy() {
        return new CNode(this.token, this.children, this.subscriptions);
    }

    public void add(INode newINode) {
        this.children.add(newINode);
    }
    public void remove(INode node) { this.children.remove(node); }

    CNode addSubscription(String clientId, Topic topic) {
        this.subscriptions.add(new Subscription(clientId, topic));
        return this;
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

    //TODO this is equivalent to negate(containsOnly(clientId))
    public boolean contains(String clientId) {
        for (Subscription sub : this.subscriptions) {
            if (sub.clientId.equals(clientId)) {
                return true;
            }
        }
        return false;
    }

    void removeSubscriptionsFor(String clientId) {
        Set<Subscription> toRemove = new HashSet<>();
        for (Subscription sub : this.subscriptions) {
            if (sub.clientId.equals(clientId)) {
                toRemove.add(sub);
            }
        }
        this.subscriptions.removeAll(toRemove);
    }
}

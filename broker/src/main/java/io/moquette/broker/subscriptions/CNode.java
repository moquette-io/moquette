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

import java.util.*;

class CNode implements Comparable<CNode> {

    private final Token token;
    private final List<INode> children;
    List<Subscription> subscriptions;

    CNode(Token token) {
        this.children = new ArrayList<>();
        this.subscriptions = new ArrayList<>();
        this.token = token;
    }

    //Copy constructor
    private CNode(Token token, List<INode> children, List<Subscription> subscriptions) {
        this.token = token; // keep reference, root comparison in directory logic relies on it for now.
        this.subscriptions = new ArrayList<>(subscriptions);
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

    private boolean equalsToken(Token token) {
        return token != null && this.token != null && this.token.equals(token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token);
    }

    CNode copy() {
        return new CNode(this.token, this.children, this.subscriptions);
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

    CNode addSubscription(Subscription newSubscription) {
        // if already contains one with same topic and same client, keep that with higher QoS
        int idx = Collections.binarySearch(subscriptions, newSubscription);
        if (idx >= 0) {
            // Subscription already exists
            final Subscription existing = subscriptions.get(idx);
            if (existing.getRequestedQos().value() < newSubscription.getRequestedQos().value()) {
                subscriptions.set(idx, newSubscription);
            }
        } else {
            this.subscriptions.add(-1 - idx, new Subscription(newSubscription));
        }
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

    @Override
    public int compareTo(CNode o) {
        return token.compareTo(o.token);
    }
}

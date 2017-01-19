/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
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

import io.moquette.spi.ISessionsStore.ClientTopicCouple;

import java.util.*;

class TreeNode {

    Token m_token;
    List<TreeNode> m_children = new ArrayList<>();
    //TODO move to set of ClientIDthe set of clientIDs that has subscriptions to this topic
    Set<ClientTopicCouple> m_subscriptions = new HashSet<>();

    TreeNode() {
    }

    Token getToken() {
        return m_token;
    }

    void setToken(Token topic) {
        this.m_token = topic;
    }

    void addSubscription(ClientTopicCouple s) {
        m_subscriptions.add(s);
    }

    void addChild(TreeNode child) {
        m_children.add(child);
    }

    /**
     * Creates a shallow copy of the current node.
     * Copy the token and the children.
     * */
    TreeNode copy() {
        final TreeNode copy = new TreeNode();
        copy.m_children = new ArrayList<>(m_children);
        copy.m_subscriptions = new HashSet<>(m_subscriptions);
        copy.m_token = m_token;
        return copy;
    }

    /**
     * Search for children that has the specified token, if not found return
     * null;
     */
    TreeNode childWithToken(Token token) {
        for (TreeNode child : m_children) {
            if (child.getToken().equals(token)) {
                return child;
            }
        }

        return null;
    }

    void updateChild(TreeNode oldChild, TreeNode newChild) {
        m_children.remove(oldChild);
        m_children.add(newChild);
    }

    Collection<ClientTopicCouple> subscriptions() {
        return m_subscriptions;
    }

    public void remove(ClientTopicCouple clientTopicCouple) {
        m_subscriptions.remove(clientTopicCouple);
    }

    List<ClientTopicCouple> matches(int pos, List<Token> tokens) {
        //check if all tokens are checked
        if (pos >= tokens.size()) {
            List<ClientTopicCouple> matchingSubs = new ArrayList<>(m_subscriptions);
            //check if it has got a MULTI child and add its subscriptions
            for (TreeNode n : m_children) {
                if (n.getToken() == Token.MULTI || n.getToken() == Token.SINGLE) {
                    matchingSubs.addAll(n.subscriptions());
                }
            }

            return matchingSubs;
        }

        //we are on MULTI, than add subscriptions and return
        if (m_token == Token.MULTI) {
            return new ArrayList<>(m_subscriptions);
        }

        int next = pos + 1;
        List<ClientTopicCouple> matchingSubs = new ArrayList<>();
        for (TreeNode n : m_children) {
            if (n.getToken().match(tokens.get(pos))) {
                matchingSubs.addAll(n.matches(next, tokens));
            }
        }

        return matchingSubs;
    }

    /**
     * Return the number of registered subscriptions
     */
    int size() {
        int res = m_subscriptions.size();
        for (TreeNode child : m_children) {
            res += child.size();
        }
        return res;
    }

    /**
     * Create a copied subtree rooted on this node but purged of clientID's subscriptions.
     * */
    TreeNode removeClientSubscriptions(String clientID) {
        //collect what to delete and then delete to avoid ConcurrentModification
        TreeNode newSubRoot = this.copy();
        List<ClientTopicCouple> subsToRemove = new ArrayList<>();
        for (ClientTopicCouple s : newSubRoot.m_subscriptions) {
            if (s.clientID.equals(clientID)) {
                subsToRemove.add(s);
            }
        }

        for (ClientTopicCouple s : subsToRemove) {
            newSubRoot.m_subscriptions.remove(s);
        }

        //go deep
        List<TreeNode> newChildren = new ArrayList<>(newSubRoot.m_children.size());
        for (TreeNode child : newSubRoot.m_children) {
            newChildren.add(child.removeClientSubscriptions(clientID));
        }
        newSubRoot.m_children = newChildren;
        return newSubRoot;
    }
}

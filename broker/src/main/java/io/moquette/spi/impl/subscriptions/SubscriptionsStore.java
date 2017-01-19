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

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import io.moquette.spi.ISessionsStore;
import io.moquette.spi.ISessionsStore.ClientTopicCouple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a tree of topics subscriptions.
 *
 * @author andrea
 */
public class SubscriptionsStore {

    public static class NodeCouple {
        final TreeNode root;
        final TreeNode createdNode;

        public NodeCouple(TreeNode root, TreeNode createdNode) {
            this.root = root;
            this.createdNode = createdNode;
        }
    }

    /**
     * Check if the topic filter of the subscription is well formed
     * @param topicFilter the filter to validate
     * @return true if it's correct.
     * */
    public static boolean validate(String topicFilter) {
        try {
            parseTopic(topicFilter);
            return true;
		} catch (ParseException pex) {
			LOG.warn("The topic filter is malformed. TopicFilter = {}, cause = {}, errorMessage = {}.", topicFilter,
					pex.getCause(), pex.getMessage());
			return false;
		}
    }

    public interface IVisitor<T> {
        void visit(TreeNode node, int deep);

        T getResult();
    }

    private class DumpTreeVisitor implements IVisitor<String> {

        String s = "";

        @Override
        public void visit(TreeNode node, int deep) {
            String subScriptionsStr = "";
            String indentTabs = indentTabs(deep);
            for (ClientTopicCouple couple : node.m_subscriptions) {
                subScriptionsStr += indentTabs + couple.toString() + "\n";
            }
            s += node.getToken() == null ? "" : node.getToken().toString();
            s +=  "\n" + (node.m_subscriptions.isEmpty() ? indentTabs : "") + subScriptionsStr /*+ "\n"*/;
        }

        private String indentTabs(int deep) {
            String s = "";
            for (int i=0; i < deep; i++) {
                s += "\t";
//                s += "--";
            }
            return s;
        }

        @Override
        public String getResult() {
            return s;
        }
    }

    private AtomicReference<TreeNode> subscriptions = new AtomicReference<>(new TreeNode());
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionsStore.class);
    private volatile ISessionsStore m_sessionsStore;

    /**
     * Initialize the subscription tree with the list of subscriptions.
     * Maintained for compatibility reasons.
     * @param sessionsStore to be used as backing store from the subscription store.
     */
    public void init(ISessionsStore sessionsStore) {
        LOG.info("Initializing subscriptions store...");
        m_sessionsStore = sessionsStore;
        List<ClientTopicCouple> subscriptions = sessionsStore.listAllSubscriptions();
        //reload any subscriptions persisted
        if (LOG.isTraceEnabled()) {
            LOG.trace("Reloading all stored subscriptions. SubscriptionTree = {}.", dumpTree());
        }

        for (ClientTopicCouple clientTopic : subscriptions) {
            LOG.info("Re-subscribing client to topic. ClientId = {}, topicFilter = {}.", clientTopic.clientID, clientTopic.topicFilter);
            add(clientTopic);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("The stored subscriptions have been reloaded. SubscriptionTree = {}.", dumpTree());
        }
    }

    public void add(ClientTopicCouple newSubscription) {
    	/*
    	 * The topic filters have already been validated at the ProtocolProcessor. We can assume they are valid.
    	 */
        TreeNode oldRoot;
        NodeCouple couple;
        do {
            oldRoot = subscriptions.get();
            couple = recreatePath(newSubscription.topicFilter, oldRoot);
            couple.createdNode.addSubscription(newSubscription); //createdNode could be null?
            //spin lock repeating till we can, swap root, if can't swap just re-do the operation
        } while(!subscriptions.compareAndSet(oldRoot, couple.root));
        LOG.debug("A subscription has been added. Root = {}, oldRoot = {}.", couple.root, oldRoot);
    }


    protected NodeCouple recreatePath(String topic, final TreeNode oldRoot) {
        List<Token> tokens;
        try {
            tokens = parseTopic(topic);
        } catch (ParseException ex) {
            //TODO handle the parse exception
			LOG.error("The topic is malformed. Topic = {}, cause = {}, errorMessage = {}.", topic, ex.getCause(),
					ex.getMessage());
			throw new IllegalArgumentException(ex.getMessage());
        }

        final TreeNode newRoot = oldRoot.copy();
        TreeNode parent = newRoot;
        TreeNode current = newRoot;
        for (Token token : tokens) {
            TreeNode matchingChildren;

            //check if a children with the same token already exists
            if ((matchingChildren = current.childWithToken(token)) != null) {
                //copy the traversed node
                current = matchingChildren.copy();
                //update the child just added in the children list
                parent.updateChild(matchingChildren, current);
                parent = current;
            } else {
                //create a new node for the newly inserted token
                matchingChildren = new TreeNode();
                matchingChildren.setToken(token);
                current.addChild(matchingChildren);
                current = matchingChildren;
            }
        }
        return new NodeCouple(newRoot, current);
    }

    public void removeSubscription(String topic, String clientID) {
    	/*
    	 * The topic filters have already been validated at the ProtocolProcessor. We can assume they are valid.
    	 */
        TreeNode oldRoot;
        NodeCouple couple;
        do {
            oldRoot = subscriptions.get();
            couple = recreatePath(topic, oldRoot);

            couple.createdNode.remove(new ClientTopicCouple(clientID, topic));
            //spin lock repeating till we can, swap root, if can't swap just re-do the operation
        } while(!subscriptions.compareAndSet(oldRoot, couple.root));
    }

    /**
     * Visit the topics tree to remove matching subscriptions with clientID.
     * It's a mutating structure operation so create a new subscription tree (partial or total).
     * @param clientID the client ID to remove.
     */
    public void removeForClient(String clientID) {
        TreeNode oldRoot;
        TreeNode newRoot;
        do {
            oldRoot = subscriptions.get();
            newRoot = oldRoot.removeClientSubscriptions(clientID);
            //spin lock repeating till we can, swap root, if can't swap just re-do the operation
        } while(!subscriptions.compareAndSet(oldRoot, newRoot));
    }


    /**
     * Given a topic string return the clients subscriptions that matches it.
     * Topic string can't contain character # and + because they are reserved to
     * listeners subscriptions, and not topic publishing.
     *
     * @param topic to use fo searching matching subscriptions.
     * @return the list of matching subscriptions, or empty if not matching.
     */
    public List<Subscription> matches(String topic) {
        List<Token> tokens;
        try {
            tokens = parseTopic(topic);
        } catch (ParseException ex) {
            //TODO handle the parse exception
            LOG.warn("The topic is malformed. Topic = {}, cause = {}, errorMessage = {}.", topic, ex.getCause(), ex.getMessage());
            return Collections.emptyList();
        }

        List<ClientTopicCouple> matchingSubs = new ArrayList<>();
        subscriptions.get().matches(0, tokens, matchingSubs);

        //remove the overlapping subscriptions, selecting ones with greatest qos
        Map<String, Subscription> subsForClient = new HashMap<>();
        for (ClientTopicCouple matchingCouple : matchingSubs) {
            Subscription existingSub = subsForClient.get(matchingCouple.clientID);
            Subscription sub = m_sessionsStore.getSubscription(matchingCouple);
            if (sub == null) {
                //if the m_sessionStore hasn't the sub because the client disconnected
                continue;
            }
            //update the selected subscriptions if not present or if has a greater qos
            if (existingSub == null || existingSub.getRequestedQos().byteValue() < sub.getRequestedQos().byteValue()) {
                subsForClient.put(matchingCouple.clientID, sub);
            }
        }
        return new ArrayList<>(subsForClient.values());
    }

    public boolean contains(Subscription sub) {
        return !matches(sub.topicFilter).isEmpty();
    }

    public int size() {
        return subscriptions.get().size();
    }

    public String dumpTree() {
        DumpTreeVisitor visitor = new DumpTreeVisitor();
        bfsVisit(subscriptions.get(), visitor, 0);
        return visitor.getResult();
    }

    private void bfsVisit(TreeNode node, IVisitor<?> visitor, int deep) {
        if (node == null) {
            return;
        }
        visitor.visit(node, deep);
        for (TreeNode child : node.m_children) {
            bfsVisit(child, visitor, ++deep);
        }
    }

    /**
     * Verify if the 2 topics matching respecting the rules of MQTT Appendix A
     * @param msgTopic the topic to match from the message
     * @param subscriptionTopic the topic filter of the subscription
     * @return true if the two topics match.
     */
    //TODO reimplement with iterators or with queues
    public static boolean matchTopics(String msgTopic, String subscriptionTopic) {
        try {
            List<Token> msgTokens = SubscriptionsStore.parseTopic(msgTopic);
            List<Token> subscriptionTokens = SubscriptionsStore.parseTopic(subscriptionTopic);
            int i = 0;
            for (; i< subscriptionTokens.size(); i++) {
                Token subToken = subscriptionTokens.get(i);
                if (subToken != Token.MULTI && subToken != Token.SINGLE) {
                    if (i >= msgTokens.size()) {
                        return false;
                    }
                    Token msgToken = msgTokens.get(i);
                    if (!msgToken.equals(subToken)) {
                        return false;
                    }
                } else {
                    if (subToken == Token.MULTI) {
                        return true;
                    }
                    if (subToken == Token.SINGLE) {
                        //skip a step forward
                    }
                }
            }
            //if last token was a SINGLE then treat it as an empty
//            if (subToken == Token.SINGLE && (i - msgTokens.size() == 1)) {
//               i--;
//            }
            return i == msgTokens.size();
        } catch (ParseException ex) {
			LOG.error(
					"The message topic, the subscription topic or both are malformed. MsgTopic = {}, subscriptionTopic = {}, cause = {}, errorMessage = {}.",
					msgTopic, subscriptionTopic, ex.getCause(), ex.getMessage());
			throw new IllegalStateException(ex.getMessage());
        }
    }

    protected static List<Token> parseTopic(String topic) throws ParseException {
        List<Token> res = new ArrayList<>();
        String[] splitted = topic.split("/");

        if (splitted.length == 0) {
            res.add(Token.EMPTY);
        }

        if (topic.endsWith("/")) {
            //Add a fictious space
            String[] newSplitted = new String[splitted.length + 1];
            System.arraycopy(splitted, 0, newSplitted, 0, splitted.length);
            newSplitted[splitted.length] = "";
            splitted = newSplitted;
        }

        for (int i = 0; i < splitted.length; i++) {
            String s = splitted[i];
            if (s.isEmpty()) {
//                if (i != 0) {
//                    throw new ParseException("Bad format of topic, expetec topic name between separators", i);
//                }
                res.add(Token.EMPTY);
            } else if (s.equals("#")) {
                //check that multi is the last symbol
                if (i != splitted.length - 1) {
                    throw new ParseException("Bad format of topic, the multi symbol (#) has to be the last one after a separator", i);
                }
                res.add(Token.MULTI);
            } else if (s.contains("#")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else if (s.equals("+")) {
                res.add(Token.SINGLE);
            } else if (s.contains("+")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else {
                res.add(new Token(s));
            }
        }

        return res;
    }
}

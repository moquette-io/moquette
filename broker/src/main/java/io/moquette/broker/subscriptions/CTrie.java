package io.moquette.broker.subscriptions;

import io.netty.handler.codec.mqtt.MqttSubscriptionOption;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

public class CTrie {

    /**
     * Models a request to subscribe a client, it's carrier for the Subscription
     * */
    public final static class SubscriptionRequest {

        private final Topic topicFilter;
        private final String clientId;
        private final MqttSubscriptionOption option;
        private boolean shared = false;
        private ShareName shareName;
        private Optional<SubscriptionIdentifier> subscriptionIdOpt;

        private SubscriptionRequest(String clientId, Topic topicFilter, MqttSubscriptionOption option, SubscriptionIdentifier subscriptionId) {
            this.topicFilter = topicFilter;
            this.clientId = clientId;
            this.option = option;
            this.subscriptionIdOpt = Optional.of(subscriptionId);
        }

        private SubscriptionRequest(String clientId, Topic topicFilter, MqttSubscriptionOption option) {
            this.topicFilter = topicFilter;
            this.clientId = clientId;
            this.option = option;
            this.subscriptionIdOpt = Optional.empty();
        }

        public static SubscriptionRequest buildNonShared(Subscription subscription) {
            return buildNonShared(subscription.clientId, subscription.topicFilter, subscription.option());
        }

        public static SubscriptionRequest buildNonShared(String clientId, Topic topicFilter, MqttSubscriptionOption option) {
            return new SubscriptionRequest(clientId, topicFilter, option);
        }

        public static SubscriptionRequest buildNonShared(String clientId, Topic topicFilter,
                                                         MqttSubscriptionOption option, SubscriptionIdentifier subscriptionId) {
            Objects.requireNonNull(subscriptionId, "SubscriptionId param can't be null");
            return new SubscriptionRequest(clientId, topicFilter, option, subscriptionId);
        }

        public static SubscriptionRequest buildShared(ShareName shareName, Topic topicFilter, String clientId,
                                                      MqttSubscriptionOption option, SubscriptionIdentifier subscriptionId) {
            Objects.requireNonNull(subscriptionId, "SubscriptionId param can't be null");
            return buildSharedHelper(shareName, topicFilter,
                () -> new SubscriptionRequest(clientId, topicFilter, option, subscriptionId));
        }

        public static SubscriptionRequest buildShared(ShareName shareName, Topic topicFilter, String clientId, MqttSubscriptionOption option) {
            return buildSharedHelper(shareName, topicFilter,
                () -> buildNonShared(clientId, topicFilter, option));
        }

        private static SubscriptionRequest buildSharedHelper(ShareName shareName, Topic topicFilter, Supplier<SubscriptionRequest> instantiator) {
            if (topicFilter.headToken().name().startsWith("$share")) {
                throw new IllegalArgumentException("Topic filter of a shared subscription can't contains $share and share name");
            }
            SubscriptionRequest request = instantiator.get();
            request.shared = true;
            request.shareName = shareName;
            return request;
        }

        public Topic getTopicFilter() {
            return topicFilter;
        }

        public MqttSubscriptionOption getOption() {
            return option;
        }

        public Subscription subscription() {
            return subscriptionIdOpt
                .map(subscriptionIdentifier -> new Subscription(clientId, topicFilter, option, subscriptionIdentifier))
                .orElseGet(() -> new Subscription(clientId, topicFilter, option));
        }

        public SharedSubscription sharedSubscription() {
            return subscriptionIdOpt
                .map(subId -> new SharedSubscription(shareName, topicFilter, clientId, option, subId))
                .orElseGet(() -> new SharedSubscription(shareName, topicFilter, clientId, option));
        }

        public boolean isShared() {
            return shared;
        }

        public ShareName getSharedName() {
            return shareName;
        }

        public String getClientId() {
            return clientId;
        }

        public boolean hasSubscriptionIdentifier() {
            return subscriptionIdOpt.isPresent();
        }

        public SubscriptionIdentifier getSubscriptionIdentifier() {
            return subscriptionIdOpt.get();
        }
    }

    /**
     * Models a request to unsubscribe a client, it's carrier for the Subscription
     * */
    public final static class UnsubscribeRequest {

        private final Topic topicFilter;
        private final String clientId;
        private boolean shared = false;
        private ShareName shareName;

        private UnsubscribeRequest(String clientId, Topic topicFilter) {
            this.topicFilter = topicFilter;
            this.clientId = clientId;
        }

        public static UnsubscribeRequest buildNonShared(String clientId, Topic topicFilter) {
            return new UnsubscribeRequest(clientId, topicFilter);
        }

        public static UnsubscribeRequest buildShared(ShareName shareName, Topic topicFilter, String clientId) {
            if (topicFilter.headToken().name().startsWith("$share")) {
                throw new IllegalArgumentException("Topic filter of a shared subscription can't contains $share and share name");
            }

            UnsubscribeRequest request = new UnsubscribeRequest(clientId, topicFilter);
            request.shared = true;
            request.shareName = shareName;
            return request;
        }

        public Topic getTopicFilter() {
            return topicFilter;
        }

        public boolean isShared() {
            return shared;
        }

        public ShareName getSharedName() {
            return shareName;
        }

        public String getClientId() {
            return clientId;
        }
    }

    interface IVisitor<T> {

        void visit(CNode node, int deep);

        T getResult();
    }

    private static final Token ROOT = new Token("root");
    private static final INode NO_PARENT = null;

    private enum Action {
        OK, REPEAT,
        OK_NEW // used to indicate that the action was successful and the subscription created a new branch
    }

    INode root;

    CTrie() {
        final CNode mainNode = new CNode(ROOT);
        this.root = new INode(mainNode);
    }

    Optional<CNode> lookup(Topic topic) {
        INode inode = this.root;
        Token token = topic.headToken();
        while (!topic.isEmpty()) {
            Optional<INode> child = inode.mainNode().childOf(token);
            if (!child.isPresent()) {
                break;
            }
            topic = topic.exceptHeadToken();
            inode = child.get();
            token = topic.headToken();
        }
        if (inode == null || !topic.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(inode.mainNode());
    }

    enum NavigationAction {
        MATCH, GODEEP, STOP
    }

    private NavigationAction evaluate(Topic topicName, CNode cnode, int depth) {
        // depth 0 is the root node of all the topics, so for topic filter
        // monitor/sensor we have <root> -> monitor -> sensor
        final boolean isFirstLevel = depth == 1;
        if (Token.MULTI.equals(cnode.getToken())) {
            Token token = topicName.headToken();
            if (token != null && token.isReserved() && isFirstLevel) {
                // [MQTT-4.7.2-1] single wildcard can't match reserved topics
                // if reserved token is the first of the topicName
                return NavigationAction.STOP;
            }
            return NavigationAction.MATCH;
        }
        if (topicName.isEmpty()) {
            return NavigationAction.STOP;
        }
        final Token token = topicName.headToken();
        if (Token.SINGLE.equals(cnode.getToken()) || cnode.getToken().equals(token) || ROOT.equals(cnode.getToken())) {
            if (Token.SINGLE.equals(cnode.getToken()) && token.isReserved() && isFirstLevel) {
                // [MQTT-4.7.2-1] single wildcard can't match reserved topics
                return NavigationAction.STOP;
            }
            return NavigationAction.GODEEP;
        }
        return NavigationAction.STOP;
    }

    public SubscriptionCollection recursiveMatch(Topic topicName) {
        SubscriptionCollection subscriptions = new SubscriptionCollection();
        recursiveMatch(topicName, this.root, 0, subscriptions);
        return subscriptions;
    }

    private void recursiveMatch(Topic topicName, INode inode, int depth, SubscriptionCollection target) {
        CNode cnode = inode.mainNode();
        if (cnode instanceof TNode) {
            return;
        }
        NavigationAction action = evaluate(topicName, cnode, depth);
        if (action == NavigationAction.MATCH) {
            target.addNormalSubscriptions(cnode.getSubscriptions());
            target.addSharedSubscriptions(cnode.getSharedSubscriptions());
            return;
        }
        if (action == NavigationAction.STOP) {
            return;
        }
        final boolean isRoot = ROOT.equals(cnode.getToken());
        final boolean isSingle = Token.SINGLE.equals(cnode.getToken());
        final boolean isMulti = Token.MULTI.equals(cnode.getToken());

        Topic remainingTopic = isRoot
            ? topicName
            : (isSingle || isMulti)
                ? topicName.exceptFullHeadToken()
                : topicName.exceptHeadToken();
        SubscriptionCollection subscriptions = new SubscriptionCollection();

        // We should only consider the maximum three children children of
        // type #, + or exact match
        Optional<INode> subInode = cnode.childOf(Token.MULTI);
        if (subInode.isPresent()) {
            recursiveMatch(remainingTopic, subInode.get(), depth + 1, target);
        }
        subInode = cnode.childOf(Token.SINGLE);
        if (subInode.isPresent()) {
            recursiveMatch(remainingTopic, subInode.get(), depth + 1, target);
        }
        if (remainingTopic.isEmpty()) {
            target.addNormalSubscriptions(cnode.getSubscriptions());
            target.addSharedSubscriptions(cnode.getSharedSubscriptions());
        } else {
            subInode = cnode.childOf(remainingTopic.headToken());
            if (subInode.isPresent()) {
                recursiveMatch(remainingTopic, subInode.get(), depth + 1, target);
            }
        }
    }

    /**
     * @return true if the subscription didn't exist.
     * */
    public boolean addToTree(SubscriptionRequest request) {
        Action res;
        do {
            res = insert(request.getTopicFilter(), this.root, request);
        } while (res == Action.REPEAT);
        return res == Action.OK_NEW;
    }

    private Action insert(Topic topic, final INode inode, SubscriptionRequest request) {
        final Token token = topic.headToken();
        final CNode cnode = inode.mainNode();
        if (!topic.isEmpty()) {
            Optional<INode> nextInode = cnode.childOf(token);
            if (nextInode.isPresent()) {
                Topic remainingTopic = topic.exceptHeadToken();
                return insert(remainingTopic, nextInode.get(), request);
            }
        }
        if (topic.isEmpty()) {
            return insertSubscription(inode, cnode, request);
        } else {
            return createNodeAndInsertSubscription(topic, inode, cnode, request);
        }
    }

    private Action insertSubscription(INode inode, CNode cnode, SubscriptionRequest newSubscription) {
        final CNode updatedCnode;
        if (cnode instanceof TNode) {
            updatedCnode = new CNode(cnode.getToken());
        } else {
            updatedCnode = cnode.copy();
        }
        updatedCnode.addSubscription(newSubscription);
        return inode.compareAndSet(cnode, updatedCnode) ? Action.OK : Action.REPEAT;
    }

    private Action createNodeAndInsertSubscription(Topic topic, INode inode, CNode cnode, SubscriptionRequest request) {
        final INode newInode = createPathRec(topic, request);
        final CNode updatedCnode;
        if (cnode instanceof TNode) {
            updatedCnode = new CNode(cnode.getToken());
        } else {
            updatedCnode = cnode.copy();
        }
        updatedCnode.add(newInode);

        return inode.compareAndSet(cnode, updatedCnode) ? Action.OK_NEW : Action.REPEAT;
    }

    private INode createPathRec(Topic topic, SubscriptionRequest request) {
        Topic remainingTopic = topic.exceptHeadToken();
        if (!remainingTopic.isEmpty()) {
            INode inode = createPathRec(remainingTopic, request);
            CNode cnode = new CNode(topic.headToken());
            cnode.add(inode);
            return new INode(cnode);
        } else {
            return createLeafNodes(topic.headToken(), request);
        }
    }

    private INode createLeafNodes(Token token, SubscriptionRequest request) {
        CNode newLeafCnode = new CNode(token);
        newLeafCnode.addSubscription(request);

        return new INode(newLeafCnode);
    }

    public void removeFromTree(UnsubscribeRequest request) {
        Action res;
        do {
            res = remove(request.getClientId(), request.getTopicFilter(), this.root, NO_PARENT, request);
        } while (res == Action.REPEAT);
    }

    private Action remove(String clientId, Topic topic, INode inode, INode iParent, UnsubscribeRequest request) {
        Token token = topic.headToken();
        final CNode cnode = inode.mainNode();
        if (!topic.isEmpty()) {
            Optional<INode> nextInode = cnode.childOf(token);
            if (nextInode.isPresent()) {
                Topic remainingTopic = topic.exceptHeadToken();
                return remove(clientId, remainingTopic, nextInode.get(), inode, request);
            }
        }
        if (cnode instanceof TNode) {
            return cleanTomb(inode, iParent);
        }
        if (cnode.containsOnly(clientId) && topic.isEmpty() && cnode.allChildren().isEmpty()) {
            // last client to leave this node, AND there are no downstream children, remove via TNode tomb
            if (inode == this.root) {
                return inode.compareAndSet(cnode, inode.mainNode().copy()) ? Action.OK : Action.REPEAT;
            }
            TNode tnode = new TNode(cnode.getToken());
            return inode.compareAndSet(cnode, tnode) ? cleanTomb(inode, iParent) : Action.REPEAT;
        } else if (cnode.contains(clientId) && topic.isEmpty()) {
            CNode updatedCnode = cnode.copy();
            updatedCnode.removeSubscriptionsFor(request);
            return inode.compareAndSet(cnode, updatedCnode) ? Action.OK : Action.REPEAT;
        } else {
            //someone else already removed
            return Action.OK;
        }
    }

    /**
     *
     * Cleans Disposes of TNode in separate Atomic CAS operation per
     * http://bravenewgeek.com/breaking-and-entering-lose-the-lock-while-embracing-concurrency/
     * We roughly follow this theory above, but we allow CNode with no Subscriptions to linger (for now).
     *
     * @param inode inode that handle to the tomb node.
     * @param iParent inode parent.
     * @return REPEAT if this method wasn't successful or OK.
     */
    private Action cleanTomb(INode inode, INode iParent) {
        CNode origCnode = iParent.mainNode();
        CNode updatedCnode = origCnode.copy();
        INode removed = updatedCnode.remove(inode);
        if (removed == inode) {
            return iParent.compareAndSet(origCnode, updatedCnode) ? Action.OK : Action.REPEAT;
        } else {
            // The node removed (from the copy!) was not the node we expected to remove.
            // Probably because another thread replaced the TNode with a live node, so
            // we don't need to clean it and can return success.
            return Action.OK;
        }
    }

    public int size() {
        SubscriptionCounterVisitor visitor = new SubscriptionCounterVisitor();
        dfsVisit(this.root, visitor, 0);
        return visitor.getResult();
    }

    public String dumpTree() {
        DumpTreeVisitor visitor = new DumpTreeVisitor();
        dfsVisit(this.root, visitor, 0);
        return visitor.getResult();
    }

    private void dfsVisit(INode node, IVisitor<?> visitor, int deep) {
        if (node == null) {
            return;
        }

        visitor.visit(node.mainNode(), deep);
        ++deep;
        for (INode child : node.mainNode().allChildren()) {
            dfsVisit(child, visitor, deep);
        }
    }
}

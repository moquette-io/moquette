package io.moquette.broker.subscriptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CTrie {

    interface IVisitor<T> {

        void visit(CNode node, int deep);

        T getResult();
    }

    private static final Token ROOT = new Token("root");
    private static final INode NO_PARENT = null;

    private enum Action {
        OK, REPEAT
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

    public List<Subscription> recursiveMatch(Topic topicName) {
        return recursiveMatch(topicName, this.root, 0);
    }

    private List<Subscription> recursiveMatch(Topic topicName, INode inode, int depth) {
        CNode cnode = inode.mainNode();
        if (cnode instanceof TNode) {
            return Collections.emptyList();
        }
        NavigationAction action = evaluate(topicName, cnode, depth);
        if (action == NavigationAction.MATCH) {
            return cnode.subscriptions;
        }
        if (action == NavigationAction.STOP) {
            return Collections.emptyList();
        }
        Topic remainingTopic = (ROOT.equals(cnode.getToken())) ? topicName : topicName.exceptHeadToken();
        List<Subscription> subscriptions = new ArrayList<>();

        // We should only consider the maximum three children children of
        // type #, + or exact match
        Optional<INode> subInode = cnode.childOf(Token.MULTI);
        if (subInode.isPresent()) {
            subscriptions.addAll(recursiveMatch(remainingTopic, subInode.get(), depth + 1));
        }
        subInode = cnode.childOf(Token.SINGLE);
        if (subInode.isPresent()) {
            subscriptions.addAll(recursiveMatch(remainingTopic, subInode.get(), depth + 1));
        }
        if (remainingTopic.isEmpty()) {
            subscriptions.addAll(cnode.subscriptions);
        } else {
            subInode = cnode.childOf(remainingTopic.headToken());
            if (subInode.isPresent()) {
                subscriptions.addAll(recursiveMatch(remainingTopic, subInode.get(), depth + 1));
            }
        }
        return subscriptions;
    }

    public void addToTree(Subscription newSubscription) {
        Action res;
        do {
            res = insert(newSubscription.topicFilter, this.root, newSubscription);
        } while (res == Action.REPEAT);
    }

    private Action insert(Topic topic, final INode inode, Subscription newSubscription) {
        final Token token = topic.headToken();
        final CNode cnode = inode.mainNode();
        if (!topic.isEmpty()) {
            Optional<INode> nextInode = cnode.childOf(token);
            if (nextInode.isPresent()) {
                Topic remainingTopic = topic.exceptHeadToken();
                return insert(remainingTopic, nextInode.get(), newSubscription);
            }
        }
        if (topic.isEmpty()) {
            return insertSubscription(inode, cnode, newSubscription);
        } else {
            return createNodeAndInsertSubscription(topic, inode, cnode, newSubscription);
        }
    }

    private Action insertSubscription(INode inode, CNode cnode, Subscription newSubscription) {
        final CNode updatedCnode;
        if (cnode instanceof TNode) {
            updatedCnode = new CNode(cnode.getToken());
        } else {
            updatedCnode = cnode.copy();
        }
        updatedCnode.addSubscription(newSubscription);
        return inode.compareAndSet(cnode, updatedCnode) ? Action.OK : Action.REPEAT;
    }

    private Action createNodeAndInsertSubscription(Topic topic, INode inode, CNode cnode, Subscription newSubscription) {
        final INode newInode = createPathRec(topic, newSubscription);
        final CNode updatedCnode;
        if (cnode instanceof TNode) {
            updatedCnode = new CNode(cnode.getToken());
        } else {
            updatedCnode = cnode.copy();
        }
        updatedCnode.add(newInode);

        return inode.compareAndSet(cnode, updatedCnode) ? Action.OK : Action.REPEAT;
    }

    private INode createPathRec(Topic topic, Subscription newSubscription) {
        Topic remainingTopic = topic.exceptHeadToken();
        if (!remainingTopic.isEmpty()) {
            INode inode = createPathRec(remainingTopic, newSubscription);
            CNode cnode = new CNode(topic.headToken());
            cnode.add(inode);
            return new INode(cnode);
        } else {
            return createLeafNodes(topic.headToken(), newSubscription);
        }
    }

    private INode createLeafNodes(Token token, Subscription newSubscription) {
        CNode newLeafCnode = new CNode(token);
        newLeafCnode.addSubscription(newSubscription);

        return new INode(newLeafCnode);
    }

    public void removeFromTree(Topic topic, String clientID) {
        Action res;
        do {
            res = remove(clientID, topic, this.root, NO_PARENT);
        } while (res == Action.REPEAT);
    }

    private Action remove(String clientId, Topic topic, INode inode, INode iParent) {
        Token token = topic.headToken();
        final CNode cnode = inode.mainNode();
        if (!topic.isEmpty()) {
            Optional<INode> nextInode = cnode.childOf(token);
            if (nextInode.isPresent()) {
                Topic remainingTopic = topic.exceptHeadToken();
                return remove(clientId, remainingTopic, nextInode.get(), inode);
            }
        }
        if (cnode instanceof TNode) {
            // this inode is a tomb, has no clients and should be cleaned up
            // Because we implemented cleanTomb below, this should be rare, but possible
            // Consider calling cleanTomb here too
            return Action.OK;
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
            updatedCnode.removeSubscriptionsFor(clientId);
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
     *
     * We roughly follow this theory above, but we allow CNode with no Subscriptions to linger (for now).
     *
     *
     * @param inode inode that handle to the tomb node.
     * @param iParent inode parent.
     * @return REPEAT if the this methods wasn't successful or OK.
     */
    private Action cleanTomb(INode inode, INode iParent) {
        CNode updatedCnode = iParent.mainNode().copy();
        updatedCnode.remove(inode);
        return iParent.compareAndSet(iParent.mainNode(), updatedCnode) ? Action.OK : Action.REPEAT;
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

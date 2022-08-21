package io.moquette.broker.subscriptions;

import java.util.*;

public class CTrie {

    interface IVisitor<T> {

        void visit(CNode node, int deep);

        T getResult();
    }

    private static final Token ROOT = new Token("root");

    private final transient Object lock = new Object();

    CNode root;

    CTrie() {
        final CNode mainNode = new CNode();
        mainNode.setToken(ROOT);
        this.root = mainNode;
    }

    Optional<CNode> lookup(Topic topic) {
        CNode currentCnode = this.root;
        CNode childCnode;
        Token token = topic.headToken();
        while (!topic.isEmpty() && (childCnode = currentCnode.getChild(token)) != null) {
            topic = topic.exceptHeadToken();
            token = topic.headToken();
            currentCnode = childCnode;
        }
        if (currentCnode == null || !topic.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(currentCnode);
    }

    enum NavigationAction {
        MATCH, GODEEP, STOP
    }

    private NavigationAction evaluate(Topic topic, CNode cnode) {
        if (Token.MULTI.equals(cnode.getToken())) {
            return NavigationAction.MATCH;
        }
        if (topic.isEmpty()) {
            return NavigationAction.STOP;
        }
        final Token token = topic.headToken();
        if (!(Token.SINGLE.equals(cnode.getToken()) || cnode.getToken().equals(token) || ROOT.equals(cnode.getToken()))) {
            return NavigationAction.STOP;
        }
        return NavigationAction.GODEEP;
    }

    public Set<Subscription> recursiveMatch(Topic topic) {
        return recursiveMatch(topic, this.root);
    }

    private Set<Subscription> recursiveMatch(Topic topic, CNode cnode) {
        if (cnode == null) {
            return Collections.emptySet();
        }
        NavigationAction action = evaluate(topic, cnode);
        if (action == NavigationAction.MATCH) {
            return cnode.allSubscription();
        }
        if (action == NavigationAction.STOP) {
            return Collections.emptySet();
        }
        Topic remainingTopic = (ROOT.equals(cnode.getToken())) ? topic : topic.exceptHeadToken();
        Set<Subscription> subscriptions = new HashSet<>();
        if (remainingTopic.isEmpty()) {
            subscriptions.addAll(cnode.allSubscription());
        }
        Iterator<Map.Entry<Token, CNode>> iterator = cnode.allChildren().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Token, CNode> child = iterator.next();
            subscriptions.addAll(recursiveMatch(remainingTopic, child.getValue()));
        }
        return subscriptions;
    }

    public void addToTree(Subscription newSubscription) {
        synchronized (lock) {
            Topic topic = newSubscription.topicFilter;
            CNode cnode = this.root;
            while (!topic.isEmpty()) {
                cnode = cnode.getChildOrDefault(topic.headToken());
                topic = topic.exceptHeadToken();
            }
            cnode.addSubscription(newSubscription);
        }
    }

    public void removeFromTree(Topic topic, String clientID) {
        synchronized (lock) {
            CNode parentCnode = null;
            CNode currCnode = this.root;
            CNode childCnode = null;
            Topic currTopic = topic;
            while (!currTopic.isEmpty() && (childCnode = currCnode.getChild(currTopic.headToken())) != null) {
                currTopic = currTopic.exceptHeadToken();
                parentCnode = currCnode;
                currCnode = childCnode;
            }
            if (childCnode == null || !currTopic.isEmpty()) {
                return;
            }
            currCnode.removeSubscription(new Subscription(clientID, topic, null));
            if (currCnode.subscriptionIsEmpty() && currCnode.childrenIsEmpty()) {
                parentCnode.removeChild(currCnode.getToken());
            }
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

    private void dfsVisit(CNode node, IVisitor<?> visitor, int deep) {
        if (node == null) {
            return;
        }

        visitor.visit(node, deep);
        ++deep;
        Iterator<Map.Entry<Token, CNode>> iterator = node.allChildren().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Token, CNode> child = iterator.next();
            dfsVisit(child.getValue(), visitor, deep);
        }
    }
}

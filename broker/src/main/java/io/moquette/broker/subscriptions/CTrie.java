package io.moquette.broker.subscriptions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class CTrie {

    interface IVisitor<T> {

        void visit(CNode node, int deep);

        T getResult();
    }

    private static final Token ROOT = new Token("root");

    CNode root;

    CTrie() {
        final CNode mainNode = new CNode();
        mainNode.setToken(ROOT);
        this.root = mainNode;
    }

    Optional<CNode> lookup(Topic topic) {
        CNode cnode = this.root;
        Token token = topic.headToken();
        CNode nextCnode;
        while (!topic.isEmpty() && (nextCnode = cnode.getChild(token)) != null) {
            topic = topic.exceptHeadToken();
            token = topic.headToken();
            cnode = nextCnode;
        }
        if (cnode == null || !topic.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(cnode);
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
        for (CNode subCnode : cnode.allChildren().values()) {
            subscriptions.addAll(recursiveMatch(remainingTopic, subCnode));
        }
        return subscriptions;
    }

    public void addToTree(Subscription newSubscription) {
        Topic topic = newSubscription.topicFilter;
        CNode cnode = this.root;
        while (!topic.isEmpty()) {
            cnode = cnode.computeChildIfAbsent(topic.headToken());
            topic = topic.exceptHeadToken();
        }
        cnode.addSubscription(newSubscription);
    }

    public void removeFromTree(Topic topic, String clientID) {
        CNode parentCnode = null;
        CNode currCnode = this.root;
        CNode childCnode = null;
        Topic parentTopic = null;
        Topic currTopic = topic;
        while (!currTopic.isEmpty() && (childCnode = currCnode.getChild(currTopic.headToken())) != null) {
            parentTopic = currTopic;
            currTopic = currTopic.exceptHeadToken();
            parentCnode = currCnode;
            currCnode = childCnode;
        }
        if (childCnode == null || !currTopic.isEmpty()) {
            return;
        }
        currCnode.removeSubscription(new Subscription(clientID, topic, null));
        parentCnode.removeEmptyChild(parentTopic.headToken());
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
        for (CNode child : node.allChildren().values()) {
            dfsVisit(child, visitor, deep);
        }
    }
}

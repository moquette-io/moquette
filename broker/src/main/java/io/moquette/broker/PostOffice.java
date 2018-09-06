package io.moquette.broker;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class PostOffice {

    private final ConcurrentMap<String, Queue> queues = new ConcurrentHashMap<>();

    void dropQueuesForClient(String clientId) {
        queues.remove(clientId);
    }

    public void fireWill(Session.Will will) {
        // TODO
    }

    public void sendQueuedMessagesWhileOffline(String clientId) {
        // TODO
    }
}

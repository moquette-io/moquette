package io.moquette.broker;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MemoryQueueRepository implements IQueueRepository {

    private Map<String, Queue<SessionRegistry.EnqueuedMessage>> queues = new HashMap<>();

    @Override
    public Set<String> listQueueNames() {
        return Collections.unmodifiableSet(queues.keySet());
    }

    @Override
    public boolean containsQueue(String queueName) {
        return queues.containsKey(queueName);
    }

    @Override
    public Queue<SessionRegistry.EnqueuedMessage> getOrCreateQueue(String clientId) {
        if (containsQueue(clientId)) {
            return queues.get(clientId);
        }

        final ConcurrentLinkedQueue<SessionRegistry.EnqueuedMessage> queue = new ConcurrentLinkedQueue<>();
        queues.put(clientId, queue);
        return queue;
    }
}

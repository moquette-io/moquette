package io.moquette.broker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MemoryQueueRepository implements IQueueRepository {

    private Map<String, Queue<SessionRegistry.EnqueuedMessage>> queues = new HashMap<>();

    @Override
    public Queue<SessionRegistry.EnqueuedMessage> createQueue(String cli, boolean clean) {
        final ConcurrentLinkedQueue<SessionRegistry.EnqueuedMessage> queue = new ConcurrentLinkedQueue<>();
        queues.put(cli, queue);
        return queue;
    }

    @Override
    public Map<String, Queue<SessionRegistry.EnqueuedMessage>> listAllQueues() {
        return Collections.unmodifiableMap(queues);
    }
}

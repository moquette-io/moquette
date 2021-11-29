package io.moquette.broker;

import io.moquette.api.EnqueuedMessage;
import io.moquette.api.IQueueRepository;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MemoryQueueRepository implements IQueueRepository {

    private Map<String, Queue<EnqueuedMessage>> queues = new HashMap<>();

    @Override
    public Queue<EnqueuedMessage> createQueue(String cli, boolean clean) {
        final ConcurrentLinkedQueue<EnqueuedMessage> queue = new ConcurrentLinkedQueue<>();
        queues.put(cli, queue);
        return queue;
    }

    @Override
    public Map<String, Queue<EnqueuedMessage>> listAllQueues() {
        return Collections.unmodifiableMap(queues);
    }
}

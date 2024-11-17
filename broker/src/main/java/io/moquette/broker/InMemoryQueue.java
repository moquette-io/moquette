package io.moquette.broker;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InMemoryQueue extends AbstractSessionMessageQueue<SessionRegistry.EnqueuedMessage> {

    private final MemoryQueueRepository queueRepository;
    private final String queueName;
    private Queue<SessionRegistry.EnqueuedMessage> queue = new ConcurrentLinkedQueue<>();

    /**
     * Constructor to create a repository untracked queue.
     */
    public InMemoryQueue() {
        this(null, null);
    }

    public InMemoryQueue(MemoryQueueRepository queueRepository, String queueName) {
        this.queueRepository = queueRepository;
        this.queueName = queueName;
    }

    @Override
    public void enqueue(SessionRegistry.EnqueuedMessage message) {
        checkEnqueuePreconditions(message);
        queue.add(message);
    }

    @Override
    public SessionRegistry.EnqueuedMessage dequeue() {
        checkDequeuePreconditions();
        return queue.poll();
    }

    @Override
    public boolean isEmpty() {
        checkIsEmptyPreconditions();
        return queue.isEmpty();
    }

    @Override
    public void closeAndPurge() {
        for (SessionRegistry.EnqueuedMessage msg : queue) {
            Utils.release(msg, "in memory queue cleanup");
        }
        if (queueRepository != null) {
            // clean up the queue from the repository
            queueRepository.dropQueue(this.queueName);
        }
        this.closed = true;
    }
}

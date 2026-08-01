package io.moquette.broker;

import io.moquette.BrokerConstants;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryQueue extends AbstractSessionMessageQueue<SessionRegistry.EnqueuedMessage> {

    private final MemoryQueueRepository queueRepository;
    private final String queueName;
    private final AtomicInteger remainingSlots = new AtomicInteger(BrokerConstants.MAX_ELEMENT_IN_QUEUE);
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
        // Adds capacity limit to the unbounded ConcurrentLinkedQueue
        int remaining = remainingSlots.decrementAndGet();
        if (remaining < 0) {
            // replenish the state and exit
            remainingSlots.incrementAndGet();
            // Like Java BlockingQueue, throw IllegalStateException if capacity has been reached
            throw new IllegalStateException("Queue capacity of " + BrokerConstants.MAX_ELEMENT_IN_QUEUE + " has been reached");
        }
        queue.add(message);
    }

    @Override
    public SessionRegistry.EnqueuedMessage dequeue() {
        checkDequeuePreconditions();
        remainingSlots.incrementAndGet();
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

package io.moquette.broker;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InMemoryQueue implements SessionMessageQueue<SessionRegistry.EnqueuedMessage> {

    private Queue<SessionRegistry.EnqueuedMessage> queue = new ConcurrentLinkedQueue<>();

    @Override
    public void enqueue(SessionRegistry.EnqueuedMessage message) {
        queue.add(message);
    }

    @Override
    public SessionRegistry.EnqueuedMessage dequeue() {
        return queue.poll();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public void close() {
        for (SessionRegistry.EnqueuedMessage msg : queue) {
            msg.release();
        }
    }
}

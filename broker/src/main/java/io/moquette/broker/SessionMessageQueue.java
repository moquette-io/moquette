package io.moquette.broker;

import java.util.Optional;

public interface SessionMessageQueue<T> {
    void enqueue(T message);

    /**
     * @return null if queue is empty.
     * */
    T dequeue();

    boolean isEmpty();

    /**
     * Executes cleanup code to release the queue.
     * */
    void close();
}

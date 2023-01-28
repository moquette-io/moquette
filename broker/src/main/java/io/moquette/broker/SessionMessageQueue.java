package io.moquette.broker;

/**
 * Queue definition used by the Session class.
 * Due to the fact that Session's code is executed in a single thread, because the
 * architecture is event loop based, the queue implementations doesn't need to be
 * thread safe.
 * */
public interface SessionMessageQueue<T> {
    void enqueue(T message);

    /**
     * @return null if queue is empty.
     * */
    T dequeue();

    boolean isEmpty();

    /**
     * Executes cleanup code to release the queue.
     * A closed queue will not accept new items and will be removed from the repository.
     * */
    void closeAndPurge();
}

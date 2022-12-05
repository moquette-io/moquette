package io.moquette.broker;

public abstract class AbstractSessionMessageQueue<T> implements SessionMessageQueue<T> {

    protected boolean closed = false;

    protected void checkEnqueuePreconditions(T t) {
        if (t == null) {
            throw new NullPointerException("Inserted element can't be null");
        }
        if (closed) {
            throw new IllegalStateException("Can't push data in a closed queue");
        }
    }

    protected void checkDequeuePreconditions() {
        if (closed) {
            throw new IllegalStateException("Can't read data from a closed queue");
        }
    }

    protected void checkIsEmptyPreconditions() {
        if (closed) {
            throw new IllegalStateException("Can't state empty status in a closed queue");
        }
    }
}

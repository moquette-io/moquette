package io.moquette.broker.unsafequeues;

public class QueueException extends Exception {

    private static final long serialVersionUID = -4782799401089093829L;

    public QueueException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueueException(String message) {
        super(message);
    }
}

package io.moquette.broker.queue;

import java.io.FileNotFoundException;

public class QueueException extends Exception {

    private static final long serialVersionUID = -4782799401089093829L;

    public QueueException(String message, Throwable cause) {
        super(message, cause);
    }
}

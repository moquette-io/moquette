package io.moquette.interception.messages;

public class InterceptExceptionMessage implements InterceptMessage {
    private Throwable error;

    public InterceptExceptionMessage(Throwable error) {
        this.error = error;
    }

    public Throwable getError() {
        return error;
    }
}

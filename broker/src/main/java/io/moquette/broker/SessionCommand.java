package io.moquette.broker;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

final class SessionCommand {

    private final String sessionId;
    private final Callable<String> action;
    private final CompletableFuture<String> task;

    public  SessionCommand(String sessionId, Callable<String> action) {
        this.sessionId = sessionId;
        this.action = action;
        this.task = new CompletableFuture<>();
    }

    public String getSessionId() {
        return this.sessionId;
    }

    public void execute() throws Exception {
        action.call();
    }

    public void complete() {
        task.complete(sessionId);
    }

    public CompletableFuture<String> completableFuture() {
        return task;
    }
}

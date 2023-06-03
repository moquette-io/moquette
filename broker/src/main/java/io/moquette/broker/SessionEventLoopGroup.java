package io.moquette.broker;

import io.moquette.interception.BrokerInterceptor;
import io.moquette.interception.messages.InterceptExceptionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.FutureTask;

class SessionEventLoopGroup {
    private static final Logger LOG = LoggerFactory.getLogger(SessionEventLoopGroup.class);

    private final SessionEventLoop[] sessionExecutors;
    private final BlockingQueue<FutureTask<String>>[] sessionQueues;
    private final int eventLoops = Runtime.getRuntime().availableProcessors();
    private final ConcurrentMap<String, Throwable> loopThrownExceptions = new ConcurrentHashMap<>();

    SessionEventLoopGroup(BrokerInterceptor interceptor, int sessionQueueSize) {
        this.sessionQueues = new BlockingQueue[eventLoops];
        for (int i = 0; i < eventLoops; i++) {
            this.sessionQueues[i] = new ArrayBlockingQueue<>(sessionQueueSize);
        }
        this.sessionExecutors = new SessionEventLoop[eventLoops];
        for (int i = 0; i < eventLoops; i++) {
            SessionEventLoop newLoop = new SessionEventLoop(this.sessionQueues[i]);
            newLoop.setName(sessionLoopName(i));
            newLoop.setUncaughtExceptionHandler((loopThread, ex) -> {
                // executed in session loop thread
                // collect the exception thrown to later re-throw
                loopThrownExceptions.put(loopThread.getName(), ex);

                // This is done in asynch from another thread in BrokerInterceptor
                interceptor.notifyLoopException(new InterceptExceptionMessage(ex));
            });
            newLoop.start();
            this.sessionExecutors[i] = newLoop;
        }
    }

    int targetQueueOrdinal(String clientId) {
        return Math.abs(clientId.hashCode()) % this.eventLoops;
    }

    private String sessionLoopName(int i) {
        return "Session Executor " + i;
    }

    String sessionLoopThreadName(String clientId) {
        final int targetQueueId = targetQueueOrdinal(clientId);
        return sessionLoopName(targetQueueId);
    }

    /**
     * Route the command to the owning SessionEventLoop
     */
    public PostOffice.RouteResult routeCommand(String clientId, String actionDescription, Callable<String> action) {
        SessionCommand cmd = new SessionCommand(clientId, action);

        if (clientId == null) {
            LOG.warn("Routing collision for action [{}]", actionDescription);
            return PostOffice.RouteResult.failed(null, "Seems awaiting new route feature completion, skipping.");
        }

        final int targetQueueId = targetQueueOrdinal(cmd.getSessionId());
        LOG.debug("Routing cmd [{}] for session [{}] to event processor {}", actionDescription, cmd.getSessionId(), targetQueueId);
        final FutureTask<String> task = new FutureTask<>(() -> {
            cmd.execute();
            cmd.complete();
            return cmd.getSessionId();
        });
        if (Thread.currentThread() == sessionExecutors[targetQueueId]) {
            SessionEventLoop.executeTask(task);
            return PostOffice.RouteResult.success(clientId, cmd.completableFuture());
        }
        if (this.sessionQueues[targetQueueId].offer(task)) {
            return PostOffice.RouteResult.success(clientId, cmd.completableFuture());
        } else {
            LOG.warn("Session command queue {} is full executing action {}", targetQueueId, actionDescription);
            return PostOffice.RouteResult.failed(clientId);
        }
    }

    public void terminate() {
        for (SessionEventLoop processor : sessionExecutors) {
            processor.interrupt();
        }
        for (SessionEventLoop processor : sessionExecutors) {
            try {
                processor.join(5_000);
            } catch (InterruptedException ex) {
                LOG.info("Interrupted while joining session event loop {}", processor.getName(), ex);
            }
        }

        for (Map.Entry<String, Throwable> loopThrownExceptionEntry : loopThrownExceptions.entrySet()) {
            String threadName = loopThrownExceptionEntry.getKey();
            Throwable threadError = loopThrownExceptionEntry.getValue();
            LOG.error("Session event loop {} terminated with error", threadName, threadError);
        }
    }

    public int getEventLoopCount() {
        return eventLoops;
    }
}

package io.moquette.broker;

import io.moquette.metrics.MetricsProvider;
import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

final class SessionEventLoop extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SessionEventLoop.class);

    private final BlockingQueue<FutureTask<String>> taskQueue;
    private final boolean flushOnExit;
    private final int queueId;
    private final MetricsProvider metricsProvider;
    private final int offerTimeoutMs;
    private final boolean timeoutUsed;
    /**
     * Allows a task to fetch the id of the session queue that is executing it.
     */
    private static final ThreadLocal<Integer> threadQueueId = new ThreadLocal<>();

    public SessionEventLoop(int queueSize, int queueId, int offerTimeoutMs, MetricsProvider metricsProvider) {
        this(new ArrayBlockingQueue<>(queueSize), queueId, offerTimeoutMs, true, metricsProvider);
    }

    /**
     * @param flushOnExit consume the commands queue before exit.
     *
     */
    public SessionEventLoop(BlockingQueue<FutureTask<String>> taskQueue, int queueId, int offerTimeoutMs, boolean flushOnExit, MetricsProvider metricsProvider) {
        this.taskQueue = taskQueue;
        this.queueId = queueId;
        this.offerTimeoutMs = offerTimeoutMs;
        this.flushOnExit = flushOnExit;
        this.metricsProvider = metricsProvider;
        this.timeoutUsed = offerTimeoutMs > 0;
    }

    public PostOffice.RouteResult addTask(String clientId, String actionDescription, SessionCommand cmd) {
        final FutureTask<String> task = new FutureTask<>(() -> {
            cmd.execute();
            cmd.complete();
            return cmd.getSessionId();
        });
        if (Thread.currentThread() == this) {
            SessionEventLoop.executeTask(task);
            return PostOffice.RouteResult.success(clientId, cmd.completableFuture());
        }
        if (taskQueue.offer(task)) {
            metricsProvider.sessionQueueInc(queueId);
            return PostOffice.RouteResult.success(clientId, cmd.completableFuture());
        } else {
            if (timeoutUsed) {
                LOG.warn("Session command queue {} is full executing action {}, retrying for max {}ms", queueId, actionDescription, offerTimeoutMs);
                try {
                    if (taskQueue.offer(task, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
                        metricsProvider.sessionQueueInc(queueId);
                        return PostOffice.RouteResult.success(clientId, cmd.completableFuture());
                    }
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted waiting too queue task");
                }
            }
            LOG.error("Session command queue {} is full executing action {}, queue failed", queueId, actionDescription);
            metricsProvider.addSessionQueueOverrun(queueId);
            return PostOffice.RouteResult.failed(clientId);
        }
    }

    @Override
    public void run() {
        threadQueueId.set(queueId);
        while (!Thread.interrupted() || (Thread.interrupted() && !taskQueue.isEmpty() && flushOnExit)) {
            try {
                // blocking call
                final FutureTask<String> task = taskQueue.take();
                metricsProvider.sessionQueueDec(queueId);
                executeTask(task);
            } catch (InterruptedException e) {
                LOG.info("SessionEventLoop {} interrupted", Thread.currentThread().getName());
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("SessionEventLoop {} exit", Thread.currentThread().getName());
    }

    public static void executeTask(final FutureTask<String> task) {
        if (!task.isCancelled()) {
            try {
                task.run();

                // we ran it, but we have to grab the exception if raised
                task.get();
            } catch (Throwable th) {
                LOG.warn("SessionEventLoop {} reached exception in processing command", Thread.currentThread().getName(), th);
                throw new RuntimeException(th);
            }
        }
    }

    public static int getThreadQueueId() {
        Integer id = threadQueueId.get();
        if (id == null) {
            return -1;
        }
        return id;
    }
}

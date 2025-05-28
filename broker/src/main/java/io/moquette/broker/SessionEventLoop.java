package io.moquette.broker;

import io.moquette.logging.MetricsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;

final class SessionEventLoop extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SessionEventLoop.class);

    private final BlockingQueue<FutureTask<String>> taskQueue;
    private final boolean flushOnExit;
    private final int queueId;

    public SessionEventLoop(BlockingQueue<FutureTask<String>> taskQueue, int queueId) {
        this(taskQueue, queueId, true);
    }

    /**
     * @param flushOnExit consume the commands queue before exit.
     * */
    public SessionEventLoop(BlockingQueue<FutureTask<String>> taskQueue, int queueId, boolean flushOnExit) {
        this.taskQueue = taskQueue;
        this.queueId = queueId;
        this.flushOnExit = flushOnExit;
    }

    @Override
    public void run() {
        while (!Thread.interrupted() || (Thread.interrupted() && !taskQueue.isEmpty() && flushOnExit)) {
            try {
                // blocking call
                final FutureTask<String> task = taskQueue.take();
                MetricsManager.getMetricsProvider().setSessionQueueFill(queueId, taskQueue.size());
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
}

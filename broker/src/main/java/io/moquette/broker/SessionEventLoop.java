package io.moquette.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.function.BiConsumer;

final class SessionEventLoop extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(SessionEventLoop.class);

    private final BlockingQueue<FutureTask<String>> taskQueue;
    private final boolean flushOnExit;
    private final BiConsumer<Thread, Throwable> exceptionHandler;

    public SessionEventLoop(BlockingQueue<FutureTask<String>> taskQueue,
                            BiConsumer<Thread, Throwable> exceptionHandler) {
        this(taskQueue, true, exceptionHandler);
    }

    public SessionEventLoop(BlockingQueue<FutureTask<String>> taskQueue) {
        this(taskQueue, true);
    }

    /**
     * @param flushOnExit consume the commands queue before exit.
     */
    public SessionEventLoop(BlockingQueue<FutureTask<String>> taskQueue, boolean flushOnExit) {
        this(taskQueue, flushOnExit, SessionEventLoop::emptyHandler);
    }

    public SessionEventLoop(BlockingQueue<FutureTask<String>> taskQueue, boolean flushOnExit, 
                            BiConsumer<Thread, Throwable> exceptionHandler) {
        Objects.nonNull(exceptionHandler);
        this.taskQueue = taskQueue;
        this.flushOnExit = flushOnExit;
        this.exceptionHandler = exceptionHandler;
    }
    
    private static void emptyHandler(Thread thread, Throwable throwable) {}

    @Override
    public void run() {
        while (!Thread.interrupted() || (Thread.interrupted() && !taskQueue.isEmpty() && flushOnExit)) {
            try {
                // blocking call
                final FutureTask<String> task = this.taskQueue.take();
                executeTask(task);
            } catch (InterruptedException e) {
                LOG.info("SessionEventLoop {} interrupted", Thread.currentThread().getName());
                Thread.currentThread().interrupt();
            } catch (Throwable th) {
                // Keep the shared loop alive on a failing command; the FutureTask already captured the
                // failure for its submitter, so co-located sessions must not be taken down with it.
                LOG.error("SessionEventLoop {} caught an unhandled error while processing a command; "
                        + "the loop continues", Thread.currentThread().getName(), th);
                exceptionHandler.accept(Thread.currentThread(), th);
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
                LOG.warn("SessionEventLoop {} reached exception in processing command", Thread.currentThread().getName()/*, th*/);
                throw new RuntimeException(th);
            }
        }
    }
}

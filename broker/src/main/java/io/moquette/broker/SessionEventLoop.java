package io.moquette.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;

final class SessionEventLoop implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SessionEventLoop.class);

    private final BlockingQueue<FutureTask<Void>> sessionQueue;

    public SessionEventLoop(BlockingQueue<FutureTask<Void>> sessionQueue) {
        this.sessionQueue = sessionQueue;
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                // blocking call
                final FutureTask<Void> task = this.sessionQueue.take();

                if (!task.isCancelled()) {
                    try {
                        task.run();

                        // we ran it, but we have to grab the exception if raised
                        task.get();
                    } catch (Throwable th) {
                        LOG.info("SessionEventLoop {} reached exception in processing command", Thread.currentThread().getName(), th);
                    }
                }
            } catch (InterruptedException e) {
                LOG.info("SessionEventLoop {} interrupted", Thread.currentThread().getName());
                Thread.currentThread().interrupt();
            }
        }
    }
}

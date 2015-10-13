package org.eclipse.moquette.server;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * This tests that the broker shuts down properly and all the threads it creates are closed.
 *
 * @author kazvictor
 */
public class ServerShutdownTest {
    private static final Logger LOG = LoggerFactory.getLogger(ServerShutdownTest.class);

    private final ScheduledExecutorService threadCounter = Executors.newScheduledThreadPool(1);

    @Test
    public void testShutdown() throws IOException, InterruptedException {
        final Set<Thread> initialThreads = Thread.getAllStackTraces().keySet();
        LOG.info("*** testShutdown ***");
        LOG.debug("Initial Thread Count = " + initialThreads.size());

        //Start the server
        Server broker = new Server();
        broker.startServer();
        //stop the server
        broker.stopServer();

        //wait till the thread count is back to the initial thread count
        final CountDownLatch threadsStoppedLatch = new CountDownLatch(1);
        threadCounter.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                LOG.debug("Current Thread Count = " + Thread.activeCount());
                if (testNoNewThreads(initialThreads)) {
                    threadsStoppedLatch.countDown();
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);

        //wait till the countdown latch is triggered. Either the threads stop or the timeout is reached.
        boolean threadsStopped = threadsStoppedLatch.await(5, TimeUnit.SECONDS);
        //shutdown the threadCounter.
        threadCounter.shutdown();

        if (!threadsStopped) {
            //print out debug messages if there are still threads running
            LOG.debug("New threads still remain. Printing remaining new threads:");
            final Set<Thread> currentThreads = Thread.getAllStackTraces().keySet();
            for (Thread thread : currentThreads) {
                if (!initialThreads.contains(thread)) {
                    LOG.debug(thread.toString());
                    for (StackTraceElement stackTraceElement : thread.getStackTrace()) {
                        LOG.debug("\t" + stackTraceElement.toString());
                    }
                }
            }
        }

        assertTrue("Broker did not shutdown properly. Not all broker threads were stopped.", threadsStopped);
    }

    /**
     * Tests is there are any new threads running that are different from the initial threads given as a parameter
     *
     * @param initialThreads The initial threads to compare the currently running threads to
     * @return returns true if there are no new threads running. returns false otherwise.
     */
    private boolean testNoNewThreads(final Set<Thread> initialThreads) {
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (!initialThreads.contains(thread) && !Thread.currentThread().equals(thread)) {
                return false;
            }
        }
        return true;
    }
}

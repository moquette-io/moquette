/*
 * Copyright 2016 jab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.moquette.server;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests that the broker shuts down properly and all the threads it creates
 * are closed.
 *
 * @author kazvictor
 * @author majcoby
 */
public class ServerShutdownIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerShutdownIntegrationTest.class);

    private final ScheduledExecutorService threadCounter = Executors.newScheduledThreadPool(1);

    @Test
    public void testShutdown() throws IOException, InterruptedException, MqttException {
        final Set<Thread> initialThreads = Thread.getAllStackTraces().keySet();
        LOG.info("*** testShutdown ***");
        LOG.debug("Initial Thread Count = " + initialThreads.size());

        //Start the server
        Server broker = new Server();
        IConfig config = new MemoryConfig(new Properties());
        broker.startServer(config, Arrays.asList(new AbstractInterceptHandler() {
        }));
        // create client and connect
        String tmpDir = System.getProperty("java.io.tmpdir");
        //connect client and do dummy publish
        MqttClientPersistence dsClient = new MqttDefaultFilePersistence(tmpDir + File.separator + "temp");
        MqttClient client = new MqttClient("tcp://localhost:1883", "publisher-test", dsClient);
        client.connect();
        client.publish("dummyTopic", new MqttMessage("dummyMessage".getBytes()));
        client.disconnect();

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
     * Tests is there are any new threads running that are different from the
     * initial threads given as a parameter
     *
     * @param initialThreads The initial threads to compare the currently
     * running threads to
     * @return returns true if there are no new threads running. returns false
     * otherwise.
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

package io.moquette.integration;

import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Fast publisher sending to slow ACK subscriber which alternates between clean session true/false
 * Inspired by issue https://github.com/moquette-io/moquette/issues/608.
 * */
public class FastPublisherSlowSubscriberTest extends AbstractIntegration {

    private static final Logger LOG = LoggerFactory.getLogger(FastPublisherSlowSubscriberTest.class);

    @TempDir
    Path tempFolder;
    private String dbPath;
    private Server broker;
    private MqttAsyncClient subscriber;
    private ScheduledExecutorService publisherPool;
    private final SecureRandom random = new SecureRandom();

    protected void startServer(String dbPath) throws IOException {
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        broker.startServer(new MemoryConfig(configProps));
    }

    @BeforeEach
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);

        publisher = createClient("publisher", tempFolder);
        publisher.connect().waitForCompletion(1_000);

        subscriber = createClient("slow_subscriber", tempFolder);
        subscriber.connect().waitForCompletion(1_000);
        publisherPool = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    @Disabled("Never ending test to be triggered by hand")
    @Test
    public void publisherAtFixedRate() throws MqttException, InterruptedException {
        CountDownLatch stopTest = new CountDownLatch(1);
        publisherPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                final int delta = random.nextInt(10);
                int temp = 15 + delta;
                try {
                    publisher.publish("/temperature", (temp + "°C").getBytes(UTF_8), 0, false);
                } catch (MqttException e) {
                    e.printStackTrace();
                    stopTest.countDown();
                    throw new RuntimeException(e);
                }
            }
        }, 1000, 100, TimeUnit.MILLISECONDS);

        slowSubscribe(1);

        stopTest.await();
    }

    @Disabled("Never ending test to be triggered by hand")
    @Test
    public void asFastAsItCan() throws MqttException, InterruptedException {
        final Thread publisherTask = new Thread() {
            @Override
            public void run() {
                while(!isInterrupted()) {
                    final int delta = random.nextInt(10);
                    int temp = 15 + delta;
                    try {
                        publisher.publish("/temperature", (temp + "°C").getBytes(UTF_8), 0, false);
                    } catch (MqttException e) {
                        e.printStackTrace();
                        interrupt();
                    }
                }
            }
        };
        publisherTask.start();

        slowSubscribe(2);

        publisherTask.join();
    }

    private void slowSubscribe(int qos) throws MqttException {
        subscriber.subscribe("/temperature", qos, new IMqttMessageListener() {
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                Thread.currentThread().sleep(500);
                final String temp = new String(message.getPayload(), UTF_8);
                LOG.info("Received temp: {}", temp);
            }
        });
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationUtils.disconnectClient(this.publisher);
        IntegrationUtils.disconnectClient(this.subscriber);

//        this.broker.stopServer();
    }
}

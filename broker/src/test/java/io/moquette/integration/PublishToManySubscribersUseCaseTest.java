package io.moquette.integration;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

import static io.moquette.BrokerConstants.FLIGHT_BEFORE_RESEND_MS;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.concurrent.Semaphore;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

// inspired by ServerIntegrationPahoTest
public class PublishToManySubscribersUseCaseTest extends AbstractIntegration {

    private static final Logger LOG = LoggerFactory.getLogger(PublishToManySubscribersUseCaseTest.class);

    private static final int COMMAND_QUEUE_SIZE = 32;

    private static final int EVENT_LOOPS = Runtime.getRuntime().availableProcessors();
    public static final int NUM_SUBSCRIBERS = COMMAND_QUEUE_SIZE * EVENT_LOOPS * 4;
    private Server broker;

    @TempDir
    Path tempFolder;
    private List<IMqttAsyncClient> subscribers;

    protected void startServer(String dbPath) throws IOException {
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        configProps.put(IConfig.SESSION_QUEUE_SIZE, Integer.toString(COMMAND_QUEUE_SIZE));
        IConfig brokerConfig = new MemoryConfig(configProps);
        broker.startServer(brokerConfig);
    }

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.ONE_SECOND);
    }

    @BeforeEach
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);

        publisher = createClient("publisher", tempFolder);
        publisher.connect().waitForCompletion(1_000);

        subscribers = createSubscribers(NUM_SUBSCRIBERS);

        CountDownLatch latch = new CountDownLatch(NUM_SUBSCRIBERS);

        final IMqttActionListener callback = createMqttCallback(latch);
        for (IMqttAsyncClient client : subscribers) {
            client.connect(null, callback);
        }

        latch.await();
        LOG.debug("After all subscriptions completed");
    }

    private IMqttActionListener createMqttCallback(CountDownLatch latch) {
        return new IMqttActionListener() {
            @Override
            public void onSuccess(IMqttToken asyncActionToken) {
                latch.countDown();
            }

            @Override
            public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                latch.countDown();
            }
        };
    }

    private IMqttActionListener createMqttCallback(Semaphore openSlots) {
        return new IMqttActionListener() {
            @Override
            public void onSuccess(IMqttToken asyncActionToken) {
                openSlots.release();
            }

            @Override
            public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                openSlots.release();
            }
        };
    }

    private List<IMqttAsyncClient> createSubscribers(int numSubscribers) throws MqttException, IOException {
        List<IMqttAsyncClient> clients = new ArrayList<>(numSubscribers);
        for (int i = 0; i < numSubscribers; i++) {
            clients.add(createClient("subscriber_" + i, tempFolder));
        }
        return clients;
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationUtils.disconnectClient(this.publisher);
        disconnectAsyncClients(this.subscribers);

        this.broker.stopServer();
    }

    private void disconnectAsyncClients(List<IMqttAsyncClient> clients) throws MqttException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(clients.size());
        final IMqttActionListener completionCallback = createMqttCallback(latch);
        for (IMqttAsyncClient client : clients) {
            IntegrationUtils.disconnectClient(client, completionCallback);
        }
        latch.await();
    }

    @Test
    void onePublishTriggerManySubscriptionsNotifications() throws MqttException, InterruptedException {
        final LongAdder receivedPublish = new LongAdder();

        //subscribe all
        segmentedParallelSubscriptions((subscriber, completionCallback) -> {
            try {
                subscriber.subscribe("/temperature", 1, null, completionCallback,
                    (String topic1, org.eclipse.paho.client.mqttv3.MqttMessage message) -> {
                        receivedPublish.increment();
                    });
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
        });

        this.publisher.publish("/temperature", "15°C".getBytes(UTF_8), 0, false);

        Awaitility.await("Waiting for resend.")
            .atMost(FLIGHT_BEFORE_RESEND_MS * 3, TimeUnit.MILLISECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .untilAdder(receivedPublish, equalTo((long) NUM_SUBSCRIBERS));
    }

    private void segmentedParallelSubscriptions(BiConsumer<IMqttAsyncClient, IMqttActionListener> biConsumer) throws InterruptedException {
        int openSlotCount = COMMAND_QUEUE_SIZE / 2;
        Semaphore openSlots = new Semaphore(openSlotCount);
        IMqttActionListener completionCallback = createMqttCallback(openSlots);
        for (IMqttAsyncClient subscriber : this.subscribers) {
            boolean haveSlot = openSlots.tryAcquire(5, TimeUnit.SECONDS);
            assertTrue(haveSlot, "Subscription ACK semaphore expired");
            biConsumer.accept(subscriber, completionCallback);
        }
        // Wait until all subscribes are completed (all slots are free again)
        boolean haveSlot = openSlots.tryAcquire(openSlotCount, 5, TimeUnit.SECONDS);
        assertTrue(haveSlot, "Subscription ACK semaphore expired");
    }
}

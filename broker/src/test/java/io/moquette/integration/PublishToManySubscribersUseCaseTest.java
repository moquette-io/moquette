package io.moquette.integration;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static io.moquette.BrokerConstants.FLIGHT_BEFORE_RESEND_MS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.equalTo;

// inspired by ServerIntegrationPahoTest
public class PublishToManySubscribersUseCaseTest {

    private static final int COMMAND_QUEUE_SIZE = 32;

    private static final int EVENT_LOOPS = Runtime.getRuntime().availableProcessors();
    public static final int NUM_SUBSCRIBERS = COMMAND_QUEUE_SIZE * EVENT_LOOPS * 4;
    private Server broker;
    private IConfig brokerConfig;

    @TempDir
    Path tempFolder;
    private String dbPath;
    private MqttClient publisher;
    private List<IMqttClient> subscribers;

    protected void startServer(String dbPath) throws IOException {
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
//        configProps.put(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, "true");
        configProps.put(BrokerConstants.SESSION_QUEUE_SIZE, Integer.toString(COMMAND_QUEUE_SIZE));
        brokerConfig = new MemoryConfig(configProps);
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

        publisher = createClient("publisher");
        publisher.connect();

        subscribers = createSubscribers(NUM_SUBSCRIBERS);
        for (IMqttClient client : subscribers) {
            client.connect();
        }
    }

    private List<IMqttClient> createSubscribers(int numSubscribers) throws MqttException, IOException {
        List<IMqttClient> clients = new ArrayList<>(numSubscribers);
        for (int i = 0; i < numSubscribers; i++) {
            clients.add(createClient("subscriber_" + i));
        }
        return clients;
    }

    private MqttClient createClient(String clientName) throws IOException, MqttException {
        final String dataPath = IntegrationUtils.newFolder(tempFolder, clientName).getAbsolutePath();
        MqttClientPersistence clientDataStore = new MqttDefaultFilePersistence(dataPath);
        return new MqttClient("tcp://localhost:1883", clientName, clientDataStore);
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationUtils.disconnectClient(this.publisher);
        disconnectClients(this.subscribers);

        this.broker.stopServer();
    }

    private void disconnectClients(List<IMqttClient> clients) throws MqttException {
        for (IMqttClient client : clients) {
            IntegrationUtils.disconnectClient(client);
        }
    }

    @Test
    void onePublishTriggerManySubscriptionsNotifications() throws MqttException {
        final LongAdder receivedPublish = new LongAdder();

        //subscribe all
        for (IMqttClient subscriber : this.subscribers) {
            subscriber.subscribe("/temperature", 1, (String topic1, org.eclipse.paho.client.mqttv3.MqttMessage message) -> {
                receivedPublish.increment();
            });
        }

        this.publisher.publish("/temperature", "15°C".getBytes(UTF_8), 0, false);

        Awaitility.await("Waiting for resend.")
            .atMost(FLIGHT_BEFORE_RESEND_MS * 3, TimeUnit.MILLISECONDS)
//            .pollDelay(FLIGHT_BEFORE_RESEND_MS * 2, TimeUnit.MILLISECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .untilAdder(receivedPublish, equalTo((long) NUM_SUBSCRIBERS));
    }
}

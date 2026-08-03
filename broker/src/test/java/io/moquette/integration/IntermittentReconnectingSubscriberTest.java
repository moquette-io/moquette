package io.moquette.integration;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class IntermittentReconnectingSubscriberTest extends AbstractServerIntegrationMixin {

    private static final Logger LOG = LoggerFactory.getLogger(IntermittentReconnectingSubscriberTest.class);

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (m_publisher != null && m_publisher.isConnected()) {
            m_publisher.disconnect();
        }

        super.tearDown();
    }

    IMqttClient m_publisher;

    private abstract class ClientSupport implements Runnable {

        protected final String clientName;
        protected MqttClient client;
        protected MqttClientPersistence dataStore;

        ClientSupport(String clientName) {
            this.clientName = clientName;
        }

        @Override
        public void run() {
            try {
                final String dataStorePath = IntegrationUtils.newFolder(tempFolder, clientName).getAbsolutePath();
                dataStore = new MqttDefaultFilePersistence(dataStorePath);

                execute();
            } catch (IOException e) {
                LOG.error("{}: client creation error", clientName, e);
            } finally {
                if (client != null && client.isConnected()) {
                    try {
                        client.disconnect();
                    } catch (MqttException e) {
                        LOG.error("{}: client closing error", clientName, e);
                    }
                }
            }
        }

        protected abstract void execute();
    }

    private class IntermittentReconnectingClient extends ClientSupport {
        IntermittentReconnectingClient(String clientName) {
            super(clientName);
        }

        @Override
        protected void execute() {
            LOG.info("Starting {} loop", clientName);
            boolean cleanSession = true;
            while (!Thread.interrupted()) {
                try {
                    client = new MqttClient("tcp://localhost:1883", clientName, dataStore);
                    MqttConnectOptions options = new MqttConnectOptions();
                    options.setCleanSession(cleanSession);

                    cleanSession = ! cleanSession;
                    client.connect(options);
                    client.subscribe("/topic", 1);

                    Thread.sleep(5000);
                    client.disconnect();
                    client.close();

                    Thread.sleep(5);
                } catch (MqttException | InterruptedException e) {
                    LOG.error("Error in executing {} ops", clientName, e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private class Publisher extends ClientSupport {

        Publisher(String clientName) {
            super(clientName);
        }

        @Override
        protected void execute() {
            LOG.info("Starting {} loop", clientName);
            try {
                client = new MqttClient("tcp://localhost:1883", clientName, dataStore);
            } catch (MqttException e) {
                LOG.error("Error in instantiating {} client", clientName, e);
                return;
            }

            // init 4K data of "a"
            byte[] data = new byte[4 * 1024];
            Arrays.fill(data, (byte) 'a');

            while (!Thread.interrupted()) {
                try {
                    client.connect();
                    for (int i = 0; i < 100; i++) {
                        client.publish("/topic", data, 1, false);
                    }

                    Thread.sleep(1);
                } catch (MqttException | InterruptedException e) {
                    LOG.error("Error in executing {} ops", clientName, e);
                    Thread.currentThread().interrupt();
                }
            }

            try {
                client.disconnect();
                client.close();
            } catch (MqttException e) {
                LOG.error("Error in closing {} client", clientName, e);
            }
        }
    }

    @Disabled("Reproduce issue #608 so must be run explicitly")
    @Test
    public void testReconnectingSubscriber() throws MqttException, IOException, InterruptedException {
        final Thread publisher = new Thread(new Publisher("Publisher"));
        publisher.start();

        final Thread intermittentSubscriber = new Thread(new IntermittentReconnectingClient("IntermittentClient"));
        intermittentSubscriber.start();

        Thread.sleep(60_000);

        intermittentSubscriber.interrupt();
        publisher.interrupt();
    }
}

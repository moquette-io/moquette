package io.moquette.integration;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

abstract class AbstractIntegration {

    String dbPath;
    MqttAsyncClient publisher;

    protected MqttAsyncClient createClient(String clientName, Path tempFolder) throws IOException, MqttException {
        final String dataPath = IntegrationUtils.newFolder(tempFolder, clientName).getAbsolutePath();
        MqttClientPersistence clientDataStore = new MqttDefaultFilePersistence(dataPath);
        return new MqttAsyncClient("tcp://localhost:1883", clientName, clientDataStore);
    }
}

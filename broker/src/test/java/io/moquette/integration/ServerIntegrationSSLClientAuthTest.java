/*
 * Copyright (c) 2012-2018 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.moquette.integration;

import io.moquette.broker.Server;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import javax.net.ssl.SSLSocketFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class ServerIntegrationSSLClientAuthTest extends ServerIntegrationSSLClientAuthBase {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationSSLClientAuthTest.class);

    Server m_server;

    IMqttClient m_client;
    MessageCollector m_callback;

    @TempDir
    Path tempFolder;

    protected void startServer(String dbPath) throws IOException {
        String file = getClass().getResource("/").getPath();
        System.setProperty("moquette.path", file);
        m_server = new Server();

        m_server.startServer(getDefaultServerProperties(dbPath));
    }

    @BeforeEach
    void setUp() throws Exception {
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        File dbFile = new File(dbPath);
        assertFalse(dbFile.exists(), String.format("The DB storagefile %s already exists", dbPath));

        startServer(dbPath);

        MqttClientPersistence subDataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "client").getAbsolutePath());
        m_client = new MqttClient("ssl://localhost:8883", "TestClient", subDataStore);
        // m_client = new MqttClient("ssl://test.mosquitto.org:8883", "TestClient", s_dataStore);

        m_callback = new MessageCollector();
        m_client.setCallback(m_callback);
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationUtils.disconnectClient(m_client);

        if (m_server != null) {
            m_server.stopServer();
        }
    }

    @Test
    public void checkClientAuthentication() throws Exception {
        LOG.info("*** checkClientAuthentication ***");
        SSLSocketFactory ssf = configureSSLSocketFactory("signedclientkeystore.jks");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setSocketFactory(ssf);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);
        m_client.disconnect();
    }

    @Test
    public void checkClientAuthenticationFail() throws Exception {
        LOG.info("*** checkClientAuthenticationFail ***");
        SSLSocketFactory ssf = configureSSLSocketFactory("unsignedclientkeystore.jks");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setSocketFactory(ssf);
        // actual a "Broken pipe" is thrown, this is not very specific.
        assertThrows(MqttException.class, () -> m_client.connect(options));
    }
}

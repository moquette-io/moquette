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

import io.moquette.BrokerConstants;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.DBAuthenticator;
import io.moquette.broker.security.DBAuthenticatorTest;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ServerIntegrationDBAuthenticatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationDBAuthenticatorTest.class);

    static DBAuthenticatorTest dbAuthenticatorTest;

    Server m_server;
    IMqttClient m_client;
    IMqttClient m_publisher;
    MessageCollector m_messagesCollector;
    IConfig m_config;

    @TempDir
    Path tempFolder;
    private MqttClientPersistence pubDataStore;

    @BeforeAll
    public static void beforeTests() throws NoSuchAlgorithmException, SQLException, ClassNotFoundException {
        dbAuthenticatorTest = new DBAuthenticatorTest();
        dbAuthenticatorTest.setup();
    }

    protected void startServer(String dbPath) throws IOException {
        m_server = new Server();
        final Properties configProps = addDBAuthenticatorConf(IntegrationUtils.prepareTestProperties(dbPath));
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }

    private void stopServer() {
        m_server.stopServer();
    }

    private Properties addDBAuthenticatorConf(Properties properties) {
        properties.put(IConfig.AUTHENTICATOR_CLASS_NAME, DBAuthenticator.class.getCanonicalName());
        properties.put(BrokerConstants.DB_AUTHENTICATOR_DRIVER, DBAuthenticatorTest.ORG_H2_DRIVER);
        properties.put(BrokerConstants.DB_AUTHENTICATOR_URL, DBAuthenticatorTest.JDBC_H2_MEM_TEST);
        properties.put(BrokerConstants.DB_AUTHENTICATOR_QUERY, "SELECT PASSWORD FROM ACCOUNT WHERE LOGIN=?");
        properties.put(BrokerConstants.DB_AUTHENTICATOR_DIGEST, DBAuthenticatorTest.SHA_256);
        return properties;
    }

    @BeforeEach
    public void setUp() throws Exception {
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);

        MqttClientPersistence dataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "client").getAbsolutePath());
        pubDataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder,"publisher").getAbsolutePath());

        m_client = new MqttClient("tcp://localhost:1883", "TestClient", dataStore);
        m_messagesCollector = new MessageCollector();
        m_client.setCallback(m_messagesCollector);

        m_publisher = new MqttClient("tcp://localhost:1883", "Publisher", pubDataStore);
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationUtils.disconnectClient(m_client);
        IntegrationUtils.disconnectClient(m_publisher);

        stopServer();
    }

    @AfterAll
    public static void shutdown() {
        dbAuthenticatorTest.teardown();
    }

    @Test
    public void connectWithValidCredentials() throws Exception {
        LOG.info("*** connectWithCredentials ***");
        m_client = new MqttClient("tcp://localhost:1883", "Publisher", pubDataStore);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName("dbuser");
        options.setPassword("password".toCharArray());
        m_client.connect(options);
        assertTrue(true);
    }

    @Test
    public void connectWithWrongCredentials() {
        LOG.info("*** connectWithWrongCredentials ***");
        try {
            m_client = new MqttClient("tcp://localhost:1883", "Publisher", pubDataStore);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName("dbuser");
            options.setPassword("wrongPassword".toCharArray());
            m_client.connect(options);
        } catch (MqttException e) {
            if (e instanceof MqttSecurityException) {
                assertTrue(true);
                return;
            } else {
                fail("Failed with exception: " + e.getMessage());
                return;
            }
        }
        fail("must not be connected. cause : wrong password given to client");
    }

}

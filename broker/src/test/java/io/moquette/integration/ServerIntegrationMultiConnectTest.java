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
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ServerIntegrationMultiConnectTest {

    static MqttConnectOptions CLEAN_SESSION_OPT = new MqttConnectOptions();
    private static PrintStream ORIG_OUT;

    Server server;
    IMqttClient client;
    IConfig configuration;
    MessageCollector m_messageCollector;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private String dbPath;
    private static ByteArrayOutputStream STRING_OUT;

    protected void startServer(String dbPath) throws IOException {
        server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        configuration = new MemoryConfig(configProps);
        server.startServer(configuration);
    }

    @BeforeClass
    public static void beforeTests() {
        STRING_OUT = new ByteArrayOutputStream();
        ORIG_OUT = System.out;
        System.setOut(new java.io.PrintStream(STRING_OUT));
        CLEAN_SESSION_OPT.setCleanSession(true);
    }

    @AfterClass
    public static void afterTests() {
        // reset the System.out
        System.setOut(ORIG_OUT);
    }

    @Before
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);

        startServer(dbPath);

        m_messageCollector = new MessageCollector();

        client = createNewClient("tcp://localhost:1883", "Client1");
    }

    private MqttClient createNewClient(String host, String clientId) throws IOException, MqttException {
        MqttClientPersistence clientDataStore = new MqttDefaultFilePersistence(tempFolder.newFolder().getAbsolutePath());
        return new MqttClient(host, clientId, clientDataStore);
    }

    @After
    public void tearDown() throws Exception {
        if (client != null && client.isConnected()) {
            client.disconnect();
        }

        server.stopServer();

        tempFolder.delete();
    }

    @Test
    public void testMultipleClientConnectsWithSameClientId() throws MqttException, IOException, InterruptedException {
        for (int i = 0 ; i < 10; i++) {
            client.connect(CLEAN_SESSION_OPT);
            MqttClient anotherClient = createNewClient("tcp://localhost:1883", "Client1");
            anotherClient.connect(CLEAN_SESSION_OPT);
            Thread.sleep(250);

            if (STRING_OUT.toString().contains("java.lang.NullPointerException")) {
                System.out.println("EXCEPTION raised \n\n" + STRING_OUT);
                fail("Found NPE on logs");
            }
        }
        assertTrue("No Exception raised in broker", true);
    }
}

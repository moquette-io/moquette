/*
 * Copyright (c) 2012-2026 The original author or authors
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
import io.moquette.broker.security.AcceptAllAuthenticator;
import io.moquette.broker.security.IAuthorizatorPolicy;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.testclient.Client;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static io.moquette.broker.ConnectionTestUtils.EMPTY_OBSERVERS;

/**
 * Regression test for GHSA-9jjc-fw8x-fmwx: an anonymous client with no write
 * permission on a topic must not be able to deliver a Will message to subscribers
 * of that topic on abrupt disconnection.
 */
public class WillUnauthorizedPublishTest {

    private static final Logger LOG = LoggerFactory.getLogger(WillUnauthorizedPublishTest.class);

    private static final String RESTRICTED_TOPIC = "restricted/topic";

    private Server server;
    private IConfig config;

    // Paho subscriber: has read access, must NOT receive the offender's will
    private IMqttClient subscriber;
    private MessageCollector messageCollector;

    // Low-level client: anonymous, no write access, sets a Will on restricted/topic
    private Client offendingClient;

    @TempDir
    Path tempFolder;

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.TWO_SECONDS);
    }

    @BeforeEach
    public void setUp() throws Exception {
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        startServer(dbPath);

        MqttClientPersistence dataStore = new MqttDefaultFilePersistence(
            IntegrationUtils.newFolder(tempFolder, "subscriber").getAbsolutePath());
        subscriber = new MqttClient("tcp://localhost:1883", "ValidSubscriber", dataStore);
        messageCollector = new MessageCollector();
        subscriber.setCallback(messageCollector);

        offendingClient = new Client("localhost");
    }

    protected void startServer(String dbPath) throws IOException {
        server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);

        // ACL: only read is permitted on restricted/topic; write is denied for everyone
        final IAuthorizatorPolicy aclPolicy = new IAuthorizatorPolicy() {
            @Override
            public boolean canWrite(Topic topic, String user, String client) {
                return !RESTRICTED_TOPIC.equals(topic.toString());
            }

            @Override
            public boolean canRead(Topic topic, String user, String client) {
                return true;
            }
        };

        server.startServer(config, EMPTY_OBSERVERS, null, new AcceptAllAuthenticator(), aclPolicy);
    }

    @AfterEach
    public void tearDown() throws Exception {
        offendingClient.close();
        try {
            if (subscriber.isConnected()) {
                subscriber.disconnect();
            }
        } catch (MqttException ex) {
            LOG.warn("subscriber disconnect error", ex);
        }
        server.stopServer();
    }

    @Test
    public void willMessageMustNotBeDeliveredWhenPublisherLacksWritePermission() throws Exception {
        LOG.info("*** willMessageMustNotBeDeliveredWhenPublisherLacksWritePermission ***");

        // Step 1 – valid subscriber connects and subscribes to the restricted topic
        subscriber.connect();
        subscriber.subscribe(RESTRICTED_TOPIC, 0);

        // Step 2 – anonymous offending client connects with a Will targeting the restricted topic
        offendingClient
            .clientId("OffendingClient")
            .connect(RESTRICTED_TOPIC, "unauthorized will payload");

        // Step 3 – offending client disconnects abruptly, which triggers the Will
        offendingClient.close();

        // Step 4 – assert the subscriber does NOT receive the will message.
        // The Will write must be subject to the same ACL check as a normal publish.
        Awaitility.await("Will message must NOT be delivered when writer lacks write permission")
            .during(Durations.ONE_SECOND)
            .atMost(Durations.TWO_SECONDS)
            .until(() -> !messageCollector.isMessageReceived());
    }
}

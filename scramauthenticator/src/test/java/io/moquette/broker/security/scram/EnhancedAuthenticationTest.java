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

package io.moquette.broker.security.scram;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests of MQTT5 enhanced authentication using SCRAM-SHA-256.
 * */
class EnhancedAuthenticationTest {

    private static final String MOQUETTE_PATH_PROPERTY = "moquette.path";

    @TempDir
    Path tempFolder;
    private Server broker;
    private String previousMoquettePath;

    @BeforeEach
    public void setUp() throws Exception {

        // the broker loads the SCRAM users from config/users.yaml under moquette.path
        previousMoquettePath = System.getProperty(MOQUETTE_PATH_PROPERTY);
        System.setProperty(MOQUETTE_PATH_PROPERTY, tempFolder.toString());
        ScramCredentialsGenerator.storeUser(tempFolder.resolve("config").resolve("users.yaml"),
            ScramCredentialsGenerator.registerNewUser("alice", "secret".toCharArray(), 4096));

        final Properties props = new Properties();
        props.put(IConfig.DATA_PATH_PROPERTY_NAME, tempFolder.toAbsolutePath() + File.separator + "moquette_store.h2");
        props.put(IConfig.PERSISTENCE_ENABLED_PROPERTY_NAME, "true");
        props.put(IConfig.PORT_PROPERTY_NAME, "1883");
        props.put(IConfig.ENABLE_TELEMETRY_NAME, "false");
        props.put(IConfig.PERSISTENT_QUEUE_TYPE_PROPERTY_NAME, "segmented");
        broker = new Server();
        broker.startServer(new MemoryConfig(props));
    }

    @AfterEach
    public void tearDown() throws Exception {
        broker.stopServer();
        if (previousMoquettePath == null) {
            System.clearProperty(MOQUETTE_PATH_PROPERTY);
        } else {
            System.setProperty(MOQUETTE_PATH_PROPERTY, previousMoquettePath);
        }
    }

    private static Mqtt5BlockingClient createScramClient(String clientId, String username, String password) {
        return MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost("localhost")
            .serverPort(1883)
            .enhancedAuth(new ScramClientAuthMechanism(username, password))
            .buildBlocking();
    }

    @Test
    public void connectWithScramSha256() {
        final Mqtt5BlockingClient client = createScramClient("scram_client", "alice", "secret");

        final Mqtt5ConnAck connAck = client.connect();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, connAck.getReasonCode(), 
            "Connection with valid SCRAM credentials must be accepted");

        client.disconnect();
    }
}

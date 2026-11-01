/*
 *
 *  * Copyright (c) 2012-2024 The original author or authors
 *  * ------------------------------------------------------
 *  * All rights reserved. This program and the accompanying materials
 *  * are made available under the terms of the Eclipse Public License v1.0
 *  * and Apache License v2.0 which accompanies this distribution.
 *  *
 *  * The Eclipse Public License is available at
 *  * http://www.eclipse.org/legal/epl-v10.html
 *  *
 *  * The Apache License v2.0 is available at
 *  * http://www.opensource.org/licenses/apache2.0.php
 *  *
 *  * You may elect to redistribute this code under either of these licenses.
 *
 */

package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import io.moquette.broker.security.scram.ScramCredentialsGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests of MQTT5 enhanced authentication using SCRAM-SHA-256.
 * */
class EnhancedAuthenticationTest extends AbstractServerIntegrationWithoutClientFixture {

    private static final String MOQUETTE_PATH_PROPERTY = "moquette.path";

    private String previousMoquettePath;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();

        // the broker loads the SCRAM users from config/users.yaml under moquette.path
        previousMoquettePath = System.getProperty(MOQUETTE_PATH_PROPERTY);
        System.setProperty(MOQUETTE_PATH_PROPERTY, tempFolder.toString());
        ScramCredentialsGenerator.storeUser(tempFolder.resolve("config").resolve("users.yaml"),
            ScramCredentialsGenerator.registerNewUser("alice", "secret".toCharArray(), 4096));
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (previousMoquettePath == null) {
            System.clearProperty(MOQUETTE_PATH_PROPERTY);
        } else {
            System.setProperty(MOQUETTE_PATH_PROPERTY, previousMoquettePath);
        }
        super.tearDown();
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

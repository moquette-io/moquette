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

package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.integration.IntegrationUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.moquette.broker.ConnectionTestUtils.EMPTY_OBSERVERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MQTT5 reproduction of the "queued messages replayed to a new principal on ClientID takeover" advisory.
 * <p>
 * A message queued for a persistent session of principal "victim" must not be delivered to principal "attacker"
 * which reconnects with the same clientId (cleanStart=false), when the attacker is not allowed to read the topic.
 */
public class SessionTakeoverQueuedMessagesMqtt5Test extends AbstractServerIntegrationWithoutClientFixture {

    private static final String SHARED_CLIENT_ID = "shared_client_id";
    private static final String SECRET_TOPIC = "tenants/victim/private";

    // real credential check: each principal has its own password
    private static final Map<String, String> PASSWORDS = new HashMap<>();
    static {
        PASSWORDS.put("victim", "victim-pwd");
        PASSWORDS.put("attacker", "attacker-pwd");
        PASSWORDS.put("publisher", "publisher-pwd");
    }

    private static final IAuthenticator AUTHENTICATOR = (clientId, username, password) ->
        username != null && password != null
            && PASSWORDS.containsKey(username)
            && PASSWORDS.get(username).equals(new String(password, StandardCharsets.UTF_8));

    private static String passwordOf(String user) {
        return PASSWORDS.get(user);
    }

    @Override
    protected void startServer(String dbPath) throws IOException {
        Properties props = IntegrationUtils.prepareTestProperties(dbPath);
        props.put(IConfig.ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
        // ACL file: only "victim" can read the secret topic, only "publisher" can write it, "attacker" has no grants
        Path aclFile = tempFolder.resolve("acl.conf");
        Files.write(aclFile, ("user victim\n"
            + "topic read " + SECRET_TOPIC + "\n"
            + "user publisher\n"
            + "topic write " + SECRET_TOPIC + "\n").getBytes(StandardCharsets.UTF_8));
        props.put(IConfig.ACL_FILE_PROPERTY_NAME, aclFile.toAbsolutePath().toString());
        broker = new Server();
        broker.startServer(new MemoryConfig(props), EMPTY_OBSERVERS, null, AUTHENTICATOR, null);
    }

    private static Mqtt5BlockingClient connectPersistent(String clientId, String user) {
        Mqtt5BlockingClient client = com.hivemq.client.mqtt.MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientId)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        Mqtt5ConnAckReasonCode code = client.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(300)
            .simpleAuth()
                .username(user)
                .password(passwordOf(user).getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
            .send()
            .getReasonCode();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, code, user + " must connect");
        return client;
    }

    @Test
    public void queuedMessagesOfVictimMustNotBeDeliveredToAnotherPrincipalTakingOverTheClientId() throws Exception {
        // victim creates a persistent session with a subscription, then goes offline keeping the session
        Mqtt5BlockingClient victim = connectPersistent(SHARED_CLIENT_ID, "victim");
        subscribeToAtQos1(victim, SECRET_TOPIC);
        victim.disconnect();

        // a message gets queued for the offline victim session
        Mqtt5BlockingClient publisher = connectPersistent("publisher", "publisher");
        publisher.publishWith().topic(SECRET_TOPIC).qos(MqttQos.AT_LEAST_ONCE)
            .payload("victim secret".getBytes(StandardCharsets.UTF_8)).send();
        publisher.disconnect();

        // attacker takes over the clientId with different credentials, resuming the session
        Mqtt5BlockingClient attacker = com.hivemq.client.mqtt.MqttClient.builder()
            .useMqttVersion5()
            .identifier(SHARED_CLIENT_ID)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = attacker.publishes(MqttGlobalPublishFilter.ALL)) {
            attacker.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(300)
                .simpleAuth()
                    .username("attacker")
                    .password(passwordOf("attacker").getBytes(StandardCharsets.UTF_8))
                    .applySimpleAuth()
                .send();

            Optional<Mqtt5Publish> leaked = publishes.receive(2, TimeUnit.SECONDS);
            assertFalse(leaked.isPresent(), "queued message of the victim must NOT be delivered to the attacker");
        } finally {
            attacker.disconnect();
        }
    }

    @Test
    public void victimReconnectingStillReceivesItsQueuedMessages() throws Exception {
        // positive control: the same principal resuming its own session must get the queued message,
        // proving the setup really queues a message and that the "no delivery" assertions aren't vacuous.
        Mqtt5BlockingClient victim = connectPersistent(SHARED_CLIENT_ID, "victim");
        subscribeToAtQos1(victim, SECRET_TOPIC);
        victim.disconnect();

        Mqtt5BlockingClient publisher = connectPersistent("publisher", "publisher");
        publisher.publishWith().topic(SECRET_TOPIC).qos(MqttQos.AT_LEAST_ONCE)
            .payload("victim secret".getBytes(StandardCharsets.UTF_8)).send();
        publisher.disconnect();

        Mqtt5BlockingClient victimReconnected = com.hivemq.client.mqtt.MqttClient.builder()
            .useMqttVersion5()
            .identifier(SHARED_CLIENT_ID)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = victimReconnected.publishes(MqttGlobalPublishFilter.ALL)) {
            victimReconnected.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(300)
                .simpleAuth()
                    .username("victim")
                    .password(passwordOf("victim").getBytes(StandardCharsets.UTF_8))
                    .applySimpleAuth()
                .send();

            Optional<Mqtt5Publish> received = publishes.receive(2, TimeUnit.SECONDS);
            assertTrue(received.isPresent(), "victim must receive its own queued message");
            verifyPayloadInUTF8(received.get(), "victim secret");
        } finally {
            victimReconnected.disconnect();
        }
    }

    @Test
    public void unacknowledgedInflightMessagesOfVictimMustNotBeResentToAnotherPrincipalTakingOverTheClientId() throws Exception {
        // victim receives the message but never acknowledges it (manual acknowledgement), so it stays in the
        // session's inflight window
        Mqtt5BlockingClient victim = connectPersistent(SHARED_CLIENT_ID, "victim");
        subscribeToAtQos1(victim, SECRET_TOPIC);
        try (Mqtt5BlockingClient.Mqtt5Publishes victimPublishes = victim.publishes(MqttGlobalPublishFilter.ALL, true)) {
            Mqtt5BlockingClient publisher = connectPersistent("publisher", "publisher");
            publisher.publishWith().topic(SECRET_TOPIC).qos(MqttQos.AT_LEAST_ONCE)
                .payload("victim secret".getBytes(StandardCharsets.UTF_8)).send();
            publisher.disconnect();

            Optional<Mqtt5Publish> received = victimPublishes.receive(2, TimeUnit.SECONDS);
            assertTrue(received.isPresent(), "victim must receive the message, without acknowledging it");
            victim.disconnect();
        }

        // attacker takes over, MQTT5 resends not acknowledged PUBLISHes right after the reconnection
        Mqtt5BlockingClient attacker = com.hivemq.client.mqtt.MqttClient.builder()
            .useMqttVersion5()
            .identifier(SHARED_CLIENT_ID)
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = attacker.publishes(MqttGlobalPublishFilter.ALL)) {
            attacker.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(300)
                .simpleAuth()
                    .username("attacker")
                    .password(passwordOf("attacker").getBytes(StandardCharsets.UTF_8))
                    .applySimpleAuth()
                .send();

            Optional<Mqtt5Publish> leaked = publishes.receive(2, TimeUnit.SECONDS);
            assertFalse(leaked.isPresent(), "unacknowledged message of the victim must NOT be resent to the attacker");
        } finally {
            attacker.disconnect();
        }
    }
}

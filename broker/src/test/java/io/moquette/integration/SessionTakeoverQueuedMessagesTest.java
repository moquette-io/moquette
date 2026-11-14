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
import io.moquette.broker.security.IAuthenticator;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.moquette.broker.ConnectionTestUtils.EMPTY_OBSERVERS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MQTT3 reproduction of the "queued messages replayed to a new principal on ClientID takeover" advisory.
 * <p>
 * A message queued for a non clean session of principal "victim" must not be delivered to principal "attacker"
 * which reconnects with the same clientId, when the attacker is not allowed to read the topic.
 */
public class SessionTakeoverQueuedMessagesTest {

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

    private Server server;
    private IMqttClient victim;
    private IMqttClient attacker;
    private IMqttClient publisher;

    @TempDir
    Path tempFolder;

    @BeforeAll
    public static void beforeTests() {
        Awaitility.setDefaultTimeout(Durations.FIVE_SECONDS);
    }

    @BeforeEach
    public void setUp() throws Exception {
        Properties props = IntegrationUtils.prepareTestProperties(IntegrationUtils.tempH2Path(tempFolder));
        props.put(IConfig.ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
        // ACL file: only "victim" can read the secret topic, only "publisher" can write it, "attacker" has no grants
        Path aclFile = tempFolder.resolve("acl.conf");
        Files.write(aclFile, ("user victim\n"
            + "topic read " + SECRET_TOPIC + "\n"
            + "user publisher\n"
            + "topic write " + SECRET_TOPIC + "\n").getBytes(StandardCharsets.UTF_8));
        props.put(IConfig.ACL_FILE_PROPERTY_NAME, aclFile.toAbsolutePath().toString());
        server = new Server();
        server.startServer(new MemoryConfig(props), EMPTY_OBSERVERS, null, AUTHENTICATOR, null);

        victim = newClient(SHARED_CLIENT_ID, "victim_store");
        attacker = newClient(SHARED_CLIENT_ID, "attacker_store");
        publisher = newClient("publisher", "publisher_store");
    }

    private IMqttClient newClient(String clientId, String storeName) throws Exception {
        MqttDefaultFilePersistence store = new MqttDefaultFilePersistence(
            IntegrationUtils.newFolder(tempFolder, storeName).getAbsolutePath());
        return new MqttClient("tcp://localhost:1883", clientId, store);
    }

    private static MqttConnectOptions options(String user, boolean cleanSession) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(user);
        options.setPassword(passwordOf(user).toCharArray());
        options.setCleanSession(cleanSession);
        return options;
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationUtils.disconnectClient(victim);
        IntegrationUtils.disconnectClient(attacker);
        IntegrationUtils.disconnectClient(publisher);
        server.stopServer();
    }

    @Test
    public void queuedMessagesOfVictimMustNotBeDeliveredToAnotherPrincipalTakingOverTheClientId() throws Exception {
        // victim creates a persistent session with a subscription, then goes offline
        victim.connect(options("victim", false));
        victim.subscribe(SECRET_TOPIC, 1);
        victim.disconnect();

        // a message gets queued for the offline victim session
        publisher.connect(options("publisher", true));
        publisher.publish(SECRET_TOPIC, "victim secret".getBytes(StandardCharsets.UTF_8), 1, false);
        publisher.disconnect();

        // attacker takes over the clientId with different credentials, resuming the session
        MessageCollector collector = new MessageCollector();
        attacker.setCallback(collector);
        attacker.connect(options("attacker", false));

        Awaitility.await("queued message of the victim must NOT be delivered to the attacker")
            .during(Durations.TWO_SECONDS)
            .atMost(Durations.FIVE_SECONDS)
            .until(() -> !collector.isMessageReceived());
    }

    @Test
    public void victimReconnectingStillReceivesItsQueuedMessages() throws Exception {
        // positive control: the same principal resuming its own session must get the queued message,
        // proving the setup really queues a message and that the "no delivery" assertions aren't vacuous.
        victim.connect(options("victim", false));
        victim.subscribe(SECRET_TOPIC, 1);
        victim.disconnect();

        publisher.connect(options("publisher", true));
        publisher.publish(SECRET_TOPIC, "victim secret".getBytes(StandardCharsets.UTF_8), 1, false);
        publisher.disconnect();

        MessageCollector collector = new MessageCollector();
        victim.setCallback(collector);
        victim.connect(options("victim", false));

        Awaitility.await("victim must receive its own queued message").until(collector::isMessageReceived);
        assertEquals("victim secret", new String(collector.retrieveMessage().getPayload(), StandardCharsets.UTF_8));
    }

    @Test
    public void unacknowledgedInflightMessagesOfVictimMustNotBeResentToAnotherPrincipalTakingOverTheClientId() throws Exception {
        // victim receives the message but never acknowledges it, so it stays in the session's inflight window
        MessageCollector victimCollector = new MessageCollector();
        victim.setManualAcks(true);
        victim.setCallback(victimCollector);
        victim.connect(options("victim", false));
        victim.subscribe(SECRET_TOPIC, 1);

        publisher.connect(options("publisher", true));
        publisher.publish(SECRET_TOPIC, "victim secret".getBytes(StandardCharsets.UTF_8), 1, false);
        publisher.disconnect();

        Awaitility.await("victim must receive the message, without acknowledging it")
            .until(victimCollector::isMessageReceived);
        victim.disconnectForcibly(100, 500);

        // attacker takes over, the broker resends not acknowledged PUBLISHes of MQTT3 sessions on a timer (5 s)
        MessageCollector attackerCollector = new MessageCollector();
        attacker.setCallback(attackerCollector);
        attacker.connect(options("attacker", false));

        Awaitility.await("unacknowledged message of the victim must NOT be resent to the attacker")
            .during(Durations.TEN_SECONDS)
            .atMost(Durations.ONE_MINUTE)
            .until(() -> !attackerCollector.isMessageReceived());
    }
}

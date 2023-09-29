/*
 * Copyright (c) 2012-2023 The original author or authors
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
import io.moquette.broker.security.PemUtils;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSocketFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ServerIntegrationSSLClientAuthCertAsUsernameTest extends ServerIntegrationSSLClientAuthBase {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationSSLClientAuthCertAsUsernameTest.class);

    Server m_server;

    IMqttClient m_client;
    @TempDir
    Path tempFolder;

    IAuthenticator authenticator;

    protected void startServer(String dbPath) throws IOException {
        String file = getClass().getResource("/").getPath();
        System.setProperty("moquette.path", file);
        m_server = new Server();

        Properties sslProps = getDefaultServerProperties(dbPath);
        sslProps.put(IConfig.PEER_CERTIFICATE_AS_USERNAME, "true");
        m_server.startServer(new MemoryConfig(sslProps), null, null,
            (clientId, username, password) -> authenticator.checkValid(clientId, username, password), null);
    }

    @BeforeEach
    void setUp() throws Exception {
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        File dbFile = new File(dbPath);
        assertFalse(dbFile.exists(), String.format("The DB storagefile %s already exists", dbPath));

        startServer(dbPath);

        MqttClientPersistence subDataStore =
            new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "client").getAbsolutePath());
        m_client = new MqttClient("ssl://localhost:8883", "TestClient", subDataStore);

        MessageCollector m_callback = new MessageCollector();
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
    public void checkClientAuthenticationPeerCertAsUsername() throws Exception {
        AtomicReference<String> usernameRef = new AtomicReference<>();
        authenticator = (clientId, username, password) -> {
            usernameRef.set(username);
            return true;
        };

        LOG.info("*** checkClientAuthenticationPeerCertAsUsername ***");
        SSLSocketFactory ssf = configureSSLSocketFactory("signedclientkeystore.jks");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setSocketFactory(ssf);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);
        m_client.disconnect();

        assertEquals(PemUtils.certificatesToPem(getClientCert("signedclientkeystore.jks", "signedtestclient")),
            usernameRef.get());
    }

    @Test
    public void checkClientAuthenticationFailPeerCertAsUsername() throws Exception {
        AtomicReference<String> usernameRef = new AtomicReference<>();
        authenticator = (clientId, username, password) -> {
            usernameRef.set(username);
            return false;
        };

        LOG.info("*** checkClientAuthenticationFailPeerCertAsUsername ***");
        SSLSocketFactory ssf = configureSSLSocketFactory("signedclientkeystore.jks");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setSocketFactory(ssf);

        MqttSecurityException ex = assertThrows(MqttSecurityException.class, () -> m_client.connect(options));
        assertEquals("Bad user name or password", ex.getMessage());
        assertEquals(PemUtils.certificatesToPem(getClientCert("signedclientkeystore.jks", "signedtestclient")),
            usernameRef.get());
    }
}

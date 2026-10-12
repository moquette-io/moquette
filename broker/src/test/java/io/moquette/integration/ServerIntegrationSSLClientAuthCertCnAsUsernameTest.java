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
import io.moquette.broker.security.IAuthenticator;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * With {@code peer_certificate_username_format=cn} the authenticator receives the subject CN of the
 * peer certificate as username.
 */
public class ServerIntegrationSSLClientAuthCertCnAsUsernameTest extends ServerIntegrationSSLClientAuthBase {

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
        sslProps.put(IConfig.PEER_CERTIFICATE_USERNAME_FORMAT, "cn");
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
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationUtils.disconnectClient(m_client);
        if (m_server != null) {
            m_server.stopServer();
        }
    }

    @Test
    public void authenticatorReceivesThePeerCertificateCnAsUsername() throws Exception {
        AtomicReference<String> usernameRef = new AtomicReference<>();
        authenticator = (clientId, username, password) -> {
            usernameRef.set(username);
            return true;
        };

        SSLSocketFactory ssf = configureSSLSocketFactory("signedclientkeystore.jks");
        MqttConnectOptions options = new MqttConnectOptions();
        options.setSocketFactory(ssf);
        m_client.connect(options);
        m_client.disconnect();

        // signedtestclient was generated with -dname cn=client.moquette.io
        assertEquals("client.moquette.io", usernameRef.get());
    }
}

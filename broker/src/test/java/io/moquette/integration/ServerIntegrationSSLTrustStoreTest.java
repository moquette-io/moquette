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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client authentication against a truststore separate from the server keystore.
 *
 * Fixtures, derived from the stores documented in {@link ServerIntegrationSSLClientAuthBase}:
 * <pre>
 *  # server identity only, no client certificates imported
 *  keytool -importkeystore -srckeystore serverkeystore.jks -srcstorepass passw0rdsrv -srcalias testserver \
 *    -srckeypass passw0rdsrv -destkeystore serveronlykeystore.jks -deststorepass passw0rdsrv \
 *    -destkeypass passw0rdsrv -deststoretype JKS
 *
 *  # the signed client's certificate only
 *  keytool -exportcert -alias signedtestclient -keystore signedclientkeystore.jks -storepass passw0rd -rfc | \
 *  keytool -importcert -noprompt -alias signedtestclient -keystore clienttruststore.jks \
 *    -storepass passw0rdtrust -storetype JKS
 * </pre>
 */
public class ServerIntegrationSSLTrustStoreTest extends ServerIntegrationSSLClientAuthBase {

    Server m_server;
    IMqttClient m_client;

    @TempDir
    Path tempFolder;

    private void startServer(String dbPath, boolean withTrustStore) throws IOException {
        String file = getClass().getResource("/").getPath();
        System.setProperty("moquette.path", file);
        m_server = new Server();

        Properties sslProps = getDefaultServerProperties(dbPath);
        sslProps.put(IConfig.JKS_PATH_PROPERTY_NAME, "src/test/resources/serveronlykeystore.jks");
        if (withTrustStore) {
            sslProps.put(IConfig.TRUST_STORE_PATH_PROPERTY_NAME, "src/test/resources/clienttruststore.jks");
            sslProps.put(IConfig.TRUST_STORE_PASSWORD_PROPERTY_NAME, "passw0rdtrust");
        }
        m_server.startServer(new MemoryConfig(sslProps));
    }

    @BeforeEach
    void setUp() throws Exception {
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

    private String freshDbPath() {
        String dbPath = IntegrationUtils.tempH2Path(tempFolder);
        assertFalse(new java.io.File(dbPath).exists(), String.format("The DB storagefile %s already exists", dbPath));
        return dbPath;
    }

    private MqttConnectOptions optionsFor(String clientKeystore) throws Exception {
        SSLSocketFactory ssf = configureSSLSocketFactory(clientKeystore);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setSocketFactory(ssf);
        return options;
    }

    @Test
    public void trustedClientConnectsThroughTheSeparateTrustStore() throws Exception {
        startServer(freshDbPath(), true);

        m_client.connect(optionsFor("signedclientkeystore.jks"));

        assertTrue(m_client.isConnected());
    }

    @Test
    public void untrustedClientIsRefusedByTheSeparateTrustStore() throws Exception {
        startServer(freshDbPath(), true);

        assertThrows(MqttException.class, () -> m_client.connect(optionsFor("unsignedclientkeystore.jks")));
        assertFalse(m_client.isConnected());
    }

    /**
     * Control: without a truststore the keystore is used, and the server-only keystore trusts no client.
     */
    @Test
    public void withoutATrustStoreTheServerOnlyKeystoreTrustsNobody() throws Exception {
        startServer(freshDbPath(), false);

        assertThrows(MqttException.class, () -> m_client.connect(optionsFor("signedclientkeystore.jks")));
        assertFalse(m_client.isConnected());
    }
}

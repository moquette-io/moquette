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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import io.moquette.broker.config.IConfig;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check that Moquette could also handle SSL.
 *
 * This test is based on a server's keystore that contains a keypair, export a certificate for the server and import it in
 * the client's keystore.
 *
 * Command executed to create the key on server's keystore:
 * <pre>
 * keytool -genkeypair -alias testserver -keyalg RSA -validity 3650 -keysize 2048 -dname cn=localhost -keystore serverkeystore.jks -keypass passw0rdsrv -storepass passw0rdsrv
 * </pre>
 *
 * Command executed to export the certificate from the server's keystore and import directly in client's keystore:
 * <pre>
 * keytool -exportcert -alias testserver -keystore serverkeystore.jks -keypass passw0rdsrv -storepass passw0rdsrv | \
 * keytool -importcert -trustcacerts -noprompt -alias testserver -keystore clientkeystore.jks -keypass passw0rd -storepass passw0rd
 * </pre>
 *
 * Tip: to verify keystore contents:
 * <pre>
 * keytool -list -v -keystore clientkeystore.jks
 * </pre>
 */
public class ServerIntegrationSSLTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationSSLTest.class);

    static String backup;

    Server m_server;
    IMqttClient m_client;
    MessageCollector m_callback;

    protected String dbPath;

    @TempDir
    Path tempFolder;

    @BeforeAll
    public static void beforeTests() {
        backup = System.getProperty("moquette.path");
    }

    @AfterAll
    public static void afterTests() {
        if (backup == null)
            System.clearProperty("moquette.path");
        else
            System.setProperty("moquette.path", backup);
    }

    protected void startServer() throws IOException {
        String file = getClass().getResource("/").getPath();
        System.setProperty("moquette.path", file);
        m_server = new Server();

        Properties sslProps = new Properties();
        sslProps.put(IConfig.SSL_PORT_PROPERTY_NAME, "8883");
        sslProps.put(IConfig.JKS_PATH_PROPERTY_NAME, "src/test/resources/serverkeystore.jks");
        sslProps.put(IConfig.KEY_STORE_PASSWORD_PROPERTY_NAME, "passw0rdsrv");
        sslProps.put(IConfig.KEY_MANAGER_PASSWORD_PROPERTY_NAME, "passw0rdsrv");
        sslProps.put(IConfig.DATA_PATH_PROPERTY_NAME, dbPath);
        sslProps.put(IConfig.PERSISTENCE_ENABLED_PROPERTY_NAME, "true");
        sslProps.put(IConfig.ENABLE_TELEMETRY_NAME, "false");
        m_server.startServer(sslProps);
    }

    @BeforeEach
    public void setUp() throws Exception {
        dbPath = IntegrationUtils.tempH2Path(tempFolder);
        File dbFile = new File(dbPath);
        assertFalse(dbFile.exists(), String.format("The DB storagefile %s already exists", dbPath));

        startServer();

        MqttClientPersistence dataStore = new MqttDefaultFilePersistence(IntegrationUtils.newFolder(tempFolder, "client").getAbsolutePath());
        m_client = new MqttClient("ssl://localhost:8883", "TestClient", dataStore);
        // m_client = new MqttClient("ssl://test.mosquitto.org:8883", "TestClient", s_dataStore);

        m_callback = new MessageCollector();
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
    public void checkSupportSSL() throws Exception {
        LOG.info("*** checkSupportSSL ***");
        SSLSocketFactory ssf = configureSSLSocketFactory();

        MqttConnectOptions options = new MqttConnectOptions();
        options.setSocketFactory(ssf);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);
        m_client.disconnect();
    }

    @Test
    public void checkSupportSSLForMultipleClient() throws Exception {
        LOG.info("*** checkSupportSSLForMultipleClient ***");
        SSLSocketFactory ssf = configureSSLSocketFactory();

        MqttConnectOptions options = new MqttConnectOptions();
        options.setSocketFactory(ssf);
        m_client.connect(options);
        m_client.subscribe("/topic", 0);

        MqttClient secondClient = new MqttClient("ssl://localhost:8883", "secondTestClient", new MemoryPersistence());
        MqttConnectOptions secondClientOptions = new MqttConnectOptions();
        secondClientOptions.setSocketFactory(ssf);
        secondClient.connect(secondClientOptions);
        secondClient.publish("/topic", new MqttMessage("message".getBytes(UTF_8)));
        secondClient.disconnect();

        m_client.disconnect();
    }

    private SSLSocketFactory configureSSLSocketFactory() throws KeyManagementException, NoSuchAlgorithmException,
            UnrecoverableKeyException, IOException, CertificateException, KeyStoreException {
        KeyStore ks = KeyStore.getInstance("JKS");
        InputStream jksInputStream = getClass().getClassLoader().getResourceAsStream("clientkeystore.jks");
        ks.load(jksInputStream, "passw0rd".toCharArray());

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, "passw0rd".toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);

        SSLContext sc = SSLContext.getInstance("TLS");
        TrustManager[] trustManagers = tmf.getTrustManagers();
        sc.init(kmf.getKeyManagers(), trustManagers, null);

        SSLSocketFactory ssf = sc.getSocketFactory();
        return ssf;
    }
}

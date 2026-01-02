/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.moquette.integration;

import io.moquette.BrokerConstants;
import io.moquette.broker.config.IConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * Base class for TLS client authentication tests which check that Moquette could also handle SSL with client
 * authentication.
 *
 * This test verifies client's authentication on server, so the server certificate has to be imported into the
 * client's keystore and the client's certificate must be imported into server's keystore.
 *
 *  the first way is done by:
 *  <pre>
 *  keytool -genkeypair -alias testserver -keyalg RSA -validity 3650 -keysize 2048 -dname cn=localhost -keystore serverkeystore.jks -keypass passw0rdsrv -storepass passw0rdsrv
 *  </pre>
 *  and
 *  <pre>
 *  keytool -exportcert -alias testserver -keystore serverkeystore.jks -keypass passw0rdsrv -storepass passw0rdsrv | \
 *  keytool -importcert -trustcacerts -noprompt -alias testserver -keystore signedclientkeystore.jks -keypass passw0rd -storepass passw0rd
 *  </pre>
 *
 *  to create the key in the client side:
 *  <pre>
 *  keytool -genkeypair -alias signedtestclient -dname cn=client.moquette.io -validity 10000 -keyalg RSA -keysize 2048 -keystore signedclientkeystore.jks -keypass passw0rd -storepass passw0rd
 *  </pre>
 *
 *  to import the client's certificate into server:
 *  <pre>
 *  keytool -exportcert -alias signedtestclient -keystore signedclientkeystore.jks -keypass passw0rd -storepass passw0rd | \
 *  keytool -importcert -trustcacerts -noprompt -alias signedtestclient -keystore serverkeystore.jks -keypass passw0rdsrv -storepass passw0rdsrv
 *  </pre>
 *
 *  To verify that a client's certficate not imported into server, it's necessary to create a client's key:
 *  <pre>
 *  keytool -genkeypair -alias unsignedtestclient -dname cn=unverifiedclient.moquette.io -validity 10000 -keyalg RSA -keysize 2048 -keystore unsignedclientkeystore.jks -keypass passw0rd -storepass passw0rd
 *  </pre>
 *  and import into it the server's certificate:
 *  <pre>
 *  keytool -exportcert -alias testserver -keystore serverkeystore.jks -keypass passw0rdsrv -storepass passw0rdsrv | \
 *  keytool -importcert -trustcacerts -noprompt -alias testserver -keystore unsignedclientkeystore.jks -keypass passw0rd -storepass passw0rd
 *  </pre>
 *
 *
 * <p>
 * Create certificates needed for client authentication
 *
 * <pre>
 * # generate integration certificate chain (valid for 10000 days)
 * keytool -genkeypair -alias signedtestserver -dname cn=moquette.eclipse.org -keyalg RSA -keysize 2048 \
 * -keystore signedserverkeystore.jks -keypass passw0rdsrv -storepass passw0rdsrv -validity 10000
 * keytool -genkeypair -alias signedtestserver_sub -dname cn=moquette.eclipse.org -keyalg RSA -keysize 2048 \
 * -keystore signedserverkeystore.jks -keypass passw0rdsrv -storepass passw0rdsrv
 *
 * # sign integration subcertificate with integration certificate (valid for 10000 days)
 * keytool -certreq -alias signedtestserver_sub -keystore signedserverkeystore.jks -keypass passw0rdsrv \
 * -storepass passw0rdsrv | \
 * keytool -gencert -alias signedtestserver -keystore signedserverkeystore.jks -keypass passw0rdsrv \
 * -storepass passw0rdsrv -validity 10000 | \
 * keytool -importcert -alias signedtestserver_sub -keystore signedserverkeystore.jks -keypass passw0rdsrv \
 * -storepass passw0rdsrv
 *
 * # generate client keypair
 * keytool -genkeypair -alias signedtestclient -dname cn=moquette.eclipse.org -keyalg RSA -keysize 2048 \
 * -keystore signedclientkeystore.jks -keypass passw0rd -storepass passw0rd
 *
 * # create signed client certificate with integration subcertificate and import to client keystore (valid for 10000 days)
 * keytool -certreq -alias signedtestclient -keystore signedclientkeystore.jks -keypass passw0rd -storepass passw0rd | \
 * keytool -gencert -alias signedtestserver_sub -keystore signedserverkeystore.jks -keypass passw0rdsrv \
 * -storepass passw0rdsrv -validity 10000 | \
 * keytool -importcert -alias signedtestclient -keystore signedclientkeystore.jks -keypass passw0rd \
 * -storepass passw0rd -noprompt
 *
 * # import integration certificates into signed truststore
 * keytool -exportcert -alias signedtestserver -keystore signedserverkeystore.jks -keypass passw0rdsrv \
 * -storepass passw0rdsrv | \
 * keytool -importcert -trustcacerts -noprompt -alias signedtestserver -keystore signedclientkeystore.jks \
 * -keypass passw0rd -storepass passw0rd
 * keytool -exportcert -alias signedtestserver_sub -keystore signedserverkeystore.jks -keypass passw0rdsrv \
 * -storepass passw0rdsrv | \
 * keytool -importcert -trustcacerts -noprompt -alias signedtestserver_sub -keystore signedclientkeystore.jks \
 * -keypass passw0rd -storepass passw0rd
 *
 * # create unsigned client certificate (valid for 10000 days)
 * keytool -genkeypair -alias unsignedtestclient -dname cn=moquette.eclipse.org -validity 10000 -keyalg RSA \
 * -keysize 2048 -keystore unsignedclientkeystore.jks -keypass passw0rd -storepass passw0rd
 *
 * # import integration certificates into unsigned truststore
 * keytool -exportcert -alias signedtestserver -keystore signedserverkeystore.jks -keypass passw0rdsrv \
 * -storepass passw0rdsrv | \
 * keytool -importcert -trustcacerts -noprompt -alias signedtestserver -keystore unsignedclientkeystore.jks \
 * -keypass passw0rd -storepass passw0rd
 * keytool -exportcert -alias signedtestserver_sub -keystore signedserverkeystore.jks -keypass passw0rdsrv \
 * -storepass passw0rdsrv | \
 * keytool -importcert -trustcacerts -noprompt -alias signedtestserver_sub -keystore unsignedclientkeystore.jks \
 * -keypass passw0rd -storepass passw0rd
 * </pre>
 * </p>
 */
public class ServerIntegrationSSLClientAuthBase {
    static String backup;

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

    static Properties getDefaultServerProperties(String dbPath) {
        Properties sslProps = new Properties();
        sslProps.put(IConfig.SSL_PORT_PROPERTY_NAME, "8883");
        sslProps.put(IConfig.JKS_PATH_PROPERTY_NAME, "src/test/resources/serverkeystore.jks");
        sslProps.put(IConfig.KEY_STORE_PASSWORD_PROPERTY_NAME, "passw0rdsrv");
        sslProps.put(IConfig.KEY_MANAGER_PASSWORD_PROPERTY_NAME, "passw0rdsrv");
        sslProps.put(IConfig.DATA_PATH_PROPERTY_NAME, dbPath);
        sslProps.put(IConfig.PERSISTENCE_ENABLED_PROPERTY_NAME, "true");
        sslProps.put(BrokerConstants.NEED_CLIENT_AUTH, "true");
        sslProps.put(IConfig.ENABLE_TELEMETRY_NAME, "false");
        return sslProps;
    }

    Certificate getClientCert(String keystore, String alias)
        throws KeyStoreException, CertificateException, IOException, NoSuchAlgorithmException {
        KeyStore ks = KeyStore.getInstance("JKS");
        InputStream jksInputStream = getClass().getClassLoader().getResourceAsStream(keystore);
        ks.load(jksInputStream, "passw0rd".toCharArray());
        return ks.getCertificate(alias);
    }

    SSLSocketFactory configureSSLSocketFactory(String keystore) throws KeyManagementException,
        NoSuchAlgorithmException, UnrecoverableKeyException, IOException, CertificateException, KeyStoreException {
        KeyStore ks = KeyStore.getInstance("JKS");
        InputStream jksInputStream = getClass().getClassLoader().getResourceAsStream(keystore);
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

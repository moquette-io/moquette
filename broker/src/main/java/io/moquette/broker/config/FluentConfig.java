package io.moquette.broker.config;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Consumer;

import static io.moquette.broker.config.IConfig.ACL_FILE_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.ALLOW_ANONYMOUS_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.AUTHENTICATOR_CLASS_NAME;
import static io.moquette.broker.config.IConfig.AUTHORIZATOR_CLASS_NAME;
import static io.moquette.broker.config.IConfig.BUFFER_FLUSH_MS_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.DATA_PATH_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE;
import static io.moquette.broker.config.IConfig.ENABLE_TELEMETRY_NAME;
import static io.moquette.broker.config.IConfig.HOST_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.JKS_PATH_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.KEY_MANAGER_PASSWORD_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.KEY_STORE_PASSWORD_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.KEY_STORE_TYPE;
import static io.moquette.broker.config.IConfig.NETTY_MAX_BYTES_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.PASSWORD_FILE_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.PEER_CERTIFICATE_AS_USERNAME;
import static io.moquette.broker.config.IConfig.PERSISTENCE_ENABLED_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.PERSISTENT_CLIENT_EXPIRATION_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.PERSISTENT_QUEUE_TYPE_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.PORT_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.RECEIVE_MAXIMUM;
import static io.moquette.broker.config.IConfig.SERVER_KEEP_ALIVE_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.SESSION_QUEUE_SIZE;
import static io.moquette.broker.config.IConfig.SSL_PORT_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.SSL_PROVIDER;
import static io.moquette.broker.config.IConfig.TOPIC_ALIAS_MAXIMUM_PROPERTY_NAME;
import static io.moquette.broker.config.IConfig.WEB_SOCKET_PORT_PROPERTY_NAME;

/**
 *  DSL to create Moquette config.
 *  It provides methods to configure every available setting.
 *  To be used instead of Properties instance used combined with MemoryConfig.
 * */
public class FluentConfig {

    private Server server;
    private TLSConfig tlsConfig;

    public enum BufferFlushKind {
        IMMEDIATE, FULL;
    }

    public enum PersistentQueueType {
        H2, SEGMENTED;
    }

    public enum SSLProvider {
        SSL, OPENSSL, OPENSSL_REFCNT;
    }

    public enum KeyStoreType {
        JKS, JCEKS, PKCS12;
    }

    private enum CreationKind {
        API, SERVER
    }

    private final Properties configAccumulator = new Properties();

    private final CreationKind creationKind;

    public FluentConfig() {
        initializeDefaultValues();
        creationKind = CreationKind.API;
    }

    // Invoked only when initialized directly by the server
    public FluentConfig(Server server) {
        initializeDefaultValues();
        creationKind = CreationKind.SERVER;
        this.server = server;
    }

    private void initializeDefaultValues() {
        // preload with default values
        configAccumulator.put(PORT_PROPERTY_NAME, Integer.toString(BrokerConstants.PORT));
        configAccumulator.put(HOST_PROPERTY_NAME, BrokerConstants.HOST);
        configAccumulator.put(PASSWORD_FILE_PROPERTY_NAME, "");
        configAccumulator.put(PEER_CERTIFICATE_AS_USERNAME, Boolean.FALSE.toString());
        configAccumulator.put(ALLOW_ANONYMOUS_PROPERTY_NAME, Boolean.TRUE.toString());
        configAccumulator.put(AUTHENTICATOR_CLASS_NAME, "");
        configAccumulator.put(AUTHORIZATOR_CLASS_NAME, "");
        configAccumulator.put(NETTY_MAX_BYTES_PROPERTY_NAME, String.valueOf(DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE));
        configAccumulator.put(PERSISTENT_QUEUE_TYPE_PROPERTY_NAME, PersistentQueueType.SEGMENTED.name().toLowerCase(Locale.ROOT));
        configAccumulator.put(DATA_PATH_PROPERTY_NAME, "data/");
        configAccumulator.put(PERSISTENCE_ENABLED_PROPERTY_NAME, Boolean.TRUE.toString());
        configAccumulator.put(BUFFER_FLUSH_MS_PROPERTY_NAME, BufferFlushKind.IMMEDIATE.name().toLowerCase(Locale.ROOT));
    }

    public FluentConfig host(String host) {
        configAccumulator.put(HOST_PROPERTY_NAME, host);
        return this;
    }

    public FluentConfig port(int port) {
        validatePort(port);
        configAccumulator.put(PORT_PROPERTY_NAME, Integer.toString(port));
        return this;
    }

    public FluentConfig websocketPort(int port) {
        validatePort(port);
        configAccumulator.put(WEB_SOCKET_PORT_PROPERTY_NAME, Integer.toString(port));
        return this;
    }

    private static void validatePort(int port) {
        if (port > 65_535 || port < 0) {
            throw new IllegalArgumentException("Port must be in range [0.65535]");
        }
    }

    public FluentConfig dataPath(String dataPath) {
        configAccumulator.put(DATA_PATH_PROPERTY_NAME, dataPath);
        return this;
    }

    public FluentConfig dataPath(Path dataPath) {
        configAccumulator.put(DATA_PATH_PROPERTY_NAME, dataPath.toAbsolutePath().toString());
        return this;
    }
    public FluentConfig enablePersistence() {
        configAccumulator.put(PERSISTENCE_ENABLED_PROPERTY_NAME, "true");
        return this;
    }

    public FluentConfig disablePersistence() {
        configAccumulator.put(PERSISTENCE_ENABLED_PROPERTY_NAME, "false");
        return this;
    }

    public FluentConfig persistentQueueType(PersistentQueueType type) {
        configAccumulator.put(PERSISTENT_QUEUE_TYPE_PROPERTY_NAME, type.name().toLowerCase(Locale.ROOT));
        return this;
    }

    public FluentConfig enablePeerCertificateAsUsername() {
        configAccumulator.put(PEER_CERTIFICATE_AS_USERNAME, "true");
        return this;
    }

    public FluentConfig disablePeerCertificateAsUsername() {
        configAccumulator.put(PEER_CERTIFICATE_AS_USERNAME, "false");
        return this;
    }

    public FluentConfig disallowAnonymous() {
        configAccumulator.put(ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
        return this;
    }

    /**
     * @param  aclPath relative path to the resource file that contains the definitions
     * */
    public FluentConfig aclFile(String aclPath) {
        configAccumulator.put(ACL_FILE_PROPERTY_NAME, aclPath);
        return this;
    }

    /**
     * @param  passwordFilePath relative path to the resource file that contains the passwords.
     * */
    public FluentConfig passwordFile(String passwordFilePath) {
        configAccumulator.put(PASSWORD_FILE_PROPERTY_NAME, passwordFilePath);
        return this;
    }
    public FluentConfig bufferFlushMillis(int value) {
        configAccumulator.put(BUFFER_FLUSH_MS_PROPERTY_NAME, Integer.valueOf(value).toString());
        return this;
    }

    public FluentConfig bufferFlushMillis(BufferFlushKind value) {
        configAccumulator.put(BUFFER_FLUSH_MS_PROPERTY_NAME, value.name().toLowerCase(Locale.ROOT));
        return this;
    }

    public FluentConfig persistentClientExpiration(String expiration) {
        configAccumulator.put(PERSISTENT_CLIENT_EXPIRATION_PROPERTY_NAME, expiration);
        return this;
    }

    public FluentConfig sessionQueueSize(int value) {
        configAccumulator.put(SESSION_QUEUE_SIZE, Integer.valueOf(value).toString());
        return this;
    }

    public FluentConfig disableTelemetry() {
        configAccumulator.put(ENABLE_TELEMETRY_NAME, "false");
        return this;
    }

    public FluentConfig enableTelemetry() {
        configAccumulator.put(ENABLE_TELEMETRY_NAME, "true");
        return this;
    }

    public FluentConfig receiveMaximum(int receiveMaximum) {
        configAccumulator.put(RECEIVE_MAXIMUM, Integer.valueOf(receiveMaximum).toString());
        return this;
    }

    public FluentConfig topicAliasMaximum(int topicAliasMaximum) {
        configAccumulator.put(TOPIC_ALIAS_MAXIMUM_PROPERTY_NAME, Integer.valueOf(topicAliasMaximum).toString());
        return this;
    }

    public FluentConfig serverKeepAlive(Duration keepAliveSeconds) {
        int seconds = (int) keepAliveSeconds.toMillis() / 1_000;
        configAccumulator.put(SERVER_KEEP_ALIVE_PROPERTY_NAME, seconds + "s");
        return this;
    }

    public class TLSConfig {

        private SSLProvider providerType;
        private KeyStoreType keyStoreType;
        private String keyStorePassword;
        private String keyManagerPassword;
        private String jksPath;
        private int sslPort;

        private TLSConfig() {}

        public void port(int port) {
            this.sslPort = port;
            validatePort(port);
        }

        public void sslProvider(SSLProvider providerType) {
            this.providerType = providerType;
        }

        public void jksPath(String jksPath) {
            this.jksPath = jksPath;
        }

        public void jksPath(Path jksPath) {
            jksPath(jksPath.toAbsolutePath().toString());
        }

        public void keyStoreType(KeyStoreType keyStoreType) {
            this.keyStoreType = keyStoreType;
        }

        /**
         * @param keyStorePassword the password to access the KeyStore
         * */
        public void keyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
        }

        /**
         * @param keyManagerPassword the password to access the key manager.
         * */
        public void keyManagerPassword(String keyManagerPassword) {
            this.keyManagerPassword = keyManagerPassword;
        }

        private String getSslProvider() {
            return providerType.name().toLowerCase(Locale.ROOT);
        }

        private String getJksPath() {
            return jksPath;
        }

        private String getKeyStoreType() {
            return keyStoreType.name().toLowerCase(Locale.ROOT);
        }
    }

    public FluentConfig withTLS(Consumer<TLSConfig> tlsBlock) {
        tlsConfig = new TLSConfig();
        tlsBlock.accept(tlsConfig);
        configAccumulator.put(SSL_PORT_PROPERTY_NAME, Integer.toString(tlsConfig.sslPort));
        configAccumulator.put(SSL_PROVIDER, tlsConfig.getSslProvider());
        configAccumulator.put(JKS_PATH_PROPERTY_NAME, tlsConfig.getJksPath());
        configAccumulator.put(KEY_STORE_TYPE, tlsConfig.getKeyStoreType());
        configAccumulator.put(KEY_STORE_PASSWORD_PROPERTY_NAME, tlsConfig.keyStorePassword);
        configAccumulator.put(KEY_MANAGER_PASSWORD_PROPERTY_NAME, tlsConfig.keyManagerPassword);

        return this;
    }

    public IConfig build() {
        if (creationKind != CreationKind.API) {
            throw new IllegalStateException("Can't build a configuration started directly by the server, use startServer method instead");
        }
        return new MemoryConfig(configAccumulator);
    }

    public Server startServer() throws IOException {
        if (creationKind != CreationKind.SERVER) {
            throw new IllegalStateException("Can't start a sever from a configuration used in API mode, use build method instead");
        }
        server.startServer(new MemoryConfig(configAccumulator));
        return server;
    }
}

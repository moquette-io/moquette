package io.moquette.broker.config;

import io.moquette.BrokerConstants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FluentConfigUsageTest {

    private void assertPropertyEquals(IConfig config, String expected, String propertyName) {
        assertEquals(expected, config.getProperty(propertyName));
    }

    @Test
    public void checkTLSSubscopeCanConfigureTheRightProperties() {
        // given
        IConfig config = new FluentConfig()
            .withTLS(tls -> {
                tls.sslProvider(FluentConfig.SSLProvider.SSL);
                tls.jksPath("/tmp/keystore.jks");
                tls.keyStoreType(FluentConfig.KeyStoreType.JKS);
                tls.keyStorePassword("s3cr3t");
                tls.keyManagerPassword("sup3rs3cr3t");
            }).build();

        // then after config build
        // expect the settings are properly set
        assertPropertyEquals(config, "ssl", BrokerConstants.SSL_PROVIDER);
        assertPropertyEquals(config, "/tmp/keystore.jks", BrokerConstants.JKS_PATH_PROPERTY_NAME);
        assertPropertyEquals(config, "jks", BrokerConstants.KEY_STORE_TYPE);
        assertPropertyEquals(config, "s3cr3t", BrokerConstants.KEY_STORE_PASSWORD_PROPERTY_NAME);
        assertPropertyEquals(config, "sup3rs3cr3t", BrokerConstants.KEY_MANAGER_PASSWORD_PROPERTY_NAME);
    }
}

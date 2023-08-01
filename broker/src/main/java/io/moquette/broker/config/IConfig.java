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

package io.moquette.broker.config;

import io.moquette.BrokerConstants;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

/**
 * Base interface for all configuration implementations (filesystem, memory or classpath)
 */
public abstract class IConfig {

    public static final String DEFAULT_CONFIG = "config/moquette.conf";
    public static final String PORT_PROPERTY_NAME = "port";
    public static final String HOST_PROPERTY_NAME = "host";
    public static final String PASSWORD_FILE_PROPERTY_NAME = "password_file";
    public static final String ALLOW_ANONYMOUS_PROPERTY_NAME = "allow_anonymous";
    public static final String AUTHENTICATOR_CLASS_NAME = "authenticator_class";
    public static final String AUTHORIZATOR_CLASS_NAME = "authorizator_class";
    public static final String PERSISTENT_QUEUE_TYPE_PROPERTY_NAME = "persistent_queue_type"; // h2 or segmented, default h2
    public static final String DATA_PATH_PROPERTY_NAME = "data_path";
    public static final String PERSISTENCE_ENABLED_PROPERTY_NAME = "persistence_enabled"; // true or false, default true
    /**
     * 0/immediate means immediate flush, like immediate_buffer_flush = true
     * -1/full means no explicit flush, let Netty flush when write buffers are full, like immediate_buffer_flush = false
     * a number of milliseconds to between flushes
     * */
    public static final String BUFFER_FLUSH_MS_PROPERTY_NAME = "buffer_flush_millis";
    public static final String WEB_SOCKET_PORT_PROPERTY_NAME = "websocket_port";
    public static final String WSS_PORT_PROPERTY_NAME = "secure_websocket_port";
    public static final String WEB_SOCKET_PATH_PROPERTY_NAME = "websocket_path";
    public static final String ACL_FILE_PROPERTY_NAME = "acl_file";
    public static final String PERSISTENT_CLIENT_EXPIRATION_PROPERTY_NAME = "persistent_client_expiration";
    public static final String SESSION_QUEUE_SIZE = "session_queue_size";
    public static final String ENABLE_TELEMETRY_NAME = "telemetry_enabled";
    /**
     * Defines the SSL implementation to use, default to "JDK".
     * @see io.netty.handler.ssl.SslProvider#name()
     */
    public static final String SSL_PROVIDER = "ssl_provider";
    public static final String SSL_PORT_PROPERTY_NAME = "ssl_port";
    public static final String JKS_PATH_PROPERTY_NAME = "jks_path";

    /** @see java.security.KeyStore#getInstance(String) for allowed types, default to "jks" */
    public static final String KEY_STORE_TYPE = "key_store_type";
    public static final String KEY_STORE_PASSWORD_PROPERTY_NAME = "key_store_password";
    public static final String KEY_MANAGER_PASSWORD_PROPERTY_NAME = "key_manager_password";
    public static final String NETTY_MAX_BYTES_PROPERTY_NAME = "netty.mqtt.message_size";
    public static final int DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE = 8092;

    public abstract void setProperty(String name, String value);

    /**
     * Same semantic of Properties
     *
     * @param name property name.
     * @return property value null if not found.
     * */
    public abstract String getProperty(String name);

    /**
     * Same semantic of Properties
     *
     * @param name property name.
     * @param defaultValue default value to return in case the property doesn't exist.
     * @return property value.
     * */
    public abstract String getProperty(String name, String defaultValue);

    void assignDefaults() {
        setProperty(PORT_PROPERTY_NAME, Integer.toString(BrokerConstants.PORT));
        setProperty(HOST_PROPERTY_NAME, BrokerConstants.HOST);
        // setProperty(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME,
        // Integer.toString(BrokerConstants.WEBSOCKET_PORT));
        setProperty(PASSWORD_FILE_PROPERTY_NAME, "");
        // setProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME,
        // BrokerConstants.DEFAULT_PERSISTENT_PATH);
        setProperty(ALLOW_ANONYMOUS_PROPERTY_NAME, Boolean.TRUE.toString());
        setProperty(AUTHENTICATOR_CLASS_NAME, "");
        setProperty(AUTHORIZATOR_CLASS_NAME, "");
        setProperty(NETTY_MAX_BYTES_PROPERTY_NAME, String.valueOf(DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE));
        setProperty(PERSISTENT_QUEUE_TYPE_PROPERTY_NAME, "segmented");
        setProperty(DATA_PATH_PROPERTY_NAME, "data/");
        setProperty(PERSISTENCE_ENABLED_PROPERTY_NAME, Boolean.TRUE.toString());
    }

    public abstract IResourceLoader getResourceLoader();

    public int intProp(String propertyName, int defaultValue) {
        String propertyValue = getProperty(propertyName);
        if (propertyValue == null) {
            return defaultValue;
        }
        return Integer.parseInt(propertyValue);
    }

    public boolean boolProp(String propertyName, boolean defaultValue) {
        String propertyValue = getProperty(propertyName);
        if (propertyValue == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(propertyValue);
    }

    public Duration durationProp(String propertyName) {
        String propertyValue = getProperty(propertyName);
        final char timeSpecifier = propertyValue.charAt(propertyValue.length() - 1);
        final TemporalUnit periodType;
        switch (timeSpecifier) {
            case 's':
                periodType = ChronoUnit.SECONDS;
                break;
            case 'm':
                periodType = ChronoUnit.MINUTES;
                break;
            case 'h':
                periodType = ChronoUnit.HOURS;
                break;
            case 'd':
                periodType = ChronoUnit.DAYS;
                break;
            case 'w':
                periodType = ChronoUnit.WEEKS;
                break;
            case 'M':
                periodType = ChronoUnit.MONTHS;
                break;
            case 'y':
                periodType = ChronoUnit.YEARS;
                break;
            default:
                throw new IllegalStateException("Can' parse duration property " + propertyName + " with value: " + propertyValue + ", admitted only h, d, w, m, y");

        }
        return Duration.of(Integer.parseInt(propertyValue.substring(0, propertyValue.length() - 1)), periodType);
    }
}

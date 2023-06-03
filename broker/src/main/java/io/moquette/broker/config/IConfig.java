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
        setProperty(BrokerConstants.PORT_PROPERTY_NAME, Integer.toString(BrokerConstants.PORT));
        setProperty(BrokerConstants.HOST_PROPERTY_NAME, BrokerConstants.HOST);
        // setProperty(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME,
        // Integer.toString(BrokerConstants.WEBSOCKET_PORT));
        setProperty(BrokerConstants.PASSWORD_FILE_PROPERTY_NAME, "");
        // setProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME,
        // BrokerConstants.DEFAULT_PERSISTENT_PATH);
        setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, Boolean.TRUE.toString());
        setProperty(BrokerConstants.AUTHENTICATOR_CLASS_NAME, "");
        setProperty(BrokerConstants.AUTHORIZATOR_CLASS_NAME, "");
        setProperty(BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
            String.valueOf(BrokerConstants.DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE));
        setProperty(BrokerConstants.PERSISTENT_QUEUE_TYPE_PROPERTY_NAME, "segmented");
        setProperty(BrokerConstants.DATA_PATH_PROPERTY_NAME, "data/");
        setProperty(BrokerConstants.PERSISTENCE_ENABLED_PROPERTY_NAME, Boolean.TRUE.toString());
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

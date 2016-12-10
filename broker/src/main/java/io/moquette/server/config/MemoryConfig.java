/*
 * Copyright (c) 2012-2015 The original author or authors
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
package io.moquette.server.config;

import io.moquette.BrokerConstants;

import java.util.Map;
import java.util.Properties;

/**
 * Configuration backed by memory.
 *
 * @author andrea
 */
public class MemoryConfig extends IConfig {

    private final Properties m_properties = new Properties();

    public MemoryConfig(Properties properties) {
        assignDefaults();
        for (Map.Entry<Object, Object> entrySet : properties.entrySet()) {
            m_properties.put(entrySet.getKey(), entrySet.getValue());
        }
    }

//    private void createDefaults() {
//        m_properties.put(BrokerConstants.PORT_PROPERTY_NAME, Integer.toString(BrokerConstants.PORT));
//        m_properties.put(BrokerConstants.HOST_PROPERTY_NAME, BrokerConstants.HOST);
//        m_properties.put(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME, Integer.toString(BrokerConstants.WEBSOCKET_PORT));
//        m_properties.put(BrokerConstants.PASSWORD_FILE_PROPERTY_NAME, "");
//        m_properties.put(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME, BrokerConstants.DEFAULT_PERSISTENT_PATH);
//        m_properties.put(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, true);
//        m_properties.put(BrokerConstants.AUTHENTICATOR_CLASS_NAME, "");
//        m_properties.put(BrokerConstants.AUTHORIZATOR_CLASS_NAME, "");
//    }

    @Override
    public void setProperty(String name, String value) {
        m_properties.setProperty(name, value);
    }

    @Override
    public String getProperty(String name) {
        return m_properties.getProperty(name);
    }

    @Override
    public String getProperty(String name, String defaultValue) {
        return m_properties.getProperty(name, defaultValue);
    }
}

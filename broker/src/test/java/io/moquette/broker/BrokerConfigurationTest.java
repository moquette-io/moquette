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
package io.moquette.broker;

import io.moquette.BrokerConstants;
import io.moquette.broker.config.MemoryConfig;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class BrokerConfigurationTest {

    @Test
    public void defaultConfig() {
        MemoryConfig config = new MemoryConfig(new Properties());
        BrokerConfiguration brokerConfiguration = new BrokerConfiguration(config);
        assertTrue(brokerConfiguration.isAllowAnonymous());
        assertFalse(brokerConfiguration.isAllowZeroByteClientId());
        assertFalse(brokerConfiguration.isReauthorizeSubscriptionsOnConnect());
        assertFalse(brokerConfiguration.isImmediateBufferFlush());
    }

    @Test
    public void configureAllowAnonymous() {
        Properties properties = new Properties();
        properties.put(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
        MemoryConfig config = new MemoryConfig(properties);
        BrokerConfiguration brokerConfiguration = new BrokerConfiguration(config);
        assertFalse(brokerConfiguration.isAllowAnonymous());
        assertFalse(brokerConfiguration.isAllowZeroByteClientId());
        assertFalse(brokerConfiguration.isReauthorizeSubscriptionsOnConnect());
        assertFalse(brokerConfiguration.isImmediateBufferFlush());
    }

    @Test
    public void configureAllowZeroByteClientId() {
        Properties properties = new Properties();
        properties.put(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, "true");
        MemoryConfig config = new MemoryConfig(properties);
        BrokerConfiguration brokerConfiguration = new BrokerConfiguration(config);
        assertTrue(brokerConfiguration.isAllowAnonymous());
        assertTrue(brokerConfiguration.isAllowZeroByteClientId());
        assertFalse(brokerConfiguration.isReauthorizeSubscriptionsOnConnect());
        assertFalse(brokerConfiguration.isImmediateBufferFlush());
    }

    @Test
    public void configureReauthorizeSubscriptionsOnConnect() {
        Properties properties = new Properties();
        properties.put(BrokerConstants.REAUTHORIZE_SUBSCRIPTIONS_ON_CONNECT, "true");
        MemoryConfig config = new MemoryConfig(properties);
        BrokerConfiguration brokerConfiguration = new BrokerConfiguration(config);
        assertTrue(brokerConfiguration.isAllowAnonymous());
        assertFalse(brokerConfiguration.isAllowZeroByteClientId());
        assertTrue(brokerConfiguration.isReauthorizeSubscriptionsOnConnect());
        assertFalse(brokerConfiguration.isImmediateBufferFlush());
    }

    @Test
    public void configureImmediateBufferFlush() {
        Properties properties = new Properties();
        properties.put(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, "true");
        MemoryConfig config = new MemoryConfig(properties);
        BrokerConfiguration brokerConfiguration = new BrokerConfiguration(config);
        assertTrue(brokerConfiguration.isAllowAnonymous());
        assertFalse(brokerConfiguration.isAllowZeroByteClientId());
        assertFalse(brokerConfiguration.isReauthorizeSubscriptionsOnConnect());
        assertTrue(brokerConfiguration.isImmediateBufferFlush());
    }
}
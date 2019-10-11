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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClasspathResourceLoaderTest {

    @Test
    public void testSetProperties() {
        IConfig classPathConfig = initConfig();

        assertEquals("" + BrokerConstants.PORT, classPathConfig.getProperty(BrokerConstants.PORT_PROPERTY_NAME));
        classPathConfig.setProperty(BrokerConstants.PORT_PROPERTY_NAME, "9999");
        assertEquals("9999", classPathConfig.getProperty(BrokerConstants.PORT_PROPERTY_NAME));
    }

    @Test
    public void testSetAuthThreadPoolSize() {
        IConfig classPathConfig = initConfig();

        assertNull(classPathConfig.getProperty(BrokerConstants.AUTH_THREAD_POOL_SIZE));
        classPathConfig.setProperty(BrokerConstants.AUTH_THREAD_POOL_SIZE, "2");
        assertEquals("2", classPathConfig.getProperty(BrokerConstants.AUTH_THREAD_POOL_SIZE));
    }


    private IConfig initConfig() {
        return new ResourceLoaderConfig(new ClasspathResourceLoader());
    }
}

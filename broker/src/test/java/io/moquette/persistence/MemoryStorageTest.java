/*
 * Copyright (c) 2012-2017 The original author or authors
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

package io.moquette.persistence;

import io.moquette.persistence.MessageStoreTCK;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.junit.After;
import org.junit.Before;

import java.util.Properties;

public class MemoryStorageTest extends MessageStoreTCK {

    MemoryStorageService storageService;

    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        IConfig conf = new MemoryConfig(props);
        storageService = new  MemoryStorageService(conf, null);
        messagesStore = storageService.messagesStore();
        sessionsStore = storageService.sessionsStore();
    }

    @After
    public void tearDown() {
        if (storageService != null) {
            storageService.close();
        }
    }
}

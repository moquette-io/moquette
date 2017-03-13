/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
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

package io.moquette.spi.persistence.lmdb;

import java.io.File;
import java.util.Properties;

import io.moquette.BrokerConstants;
import io.moquette.server.IntegrationUtils;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.moquette.spi.persistence.AbstractPersistentStoreTest;

/**
 *
 * @author Jan Zajic
 */
public class LMDBPersistentStoreTest extends AbstractPersistentStoreTest {

    @Override
    protected LMDBPersistentStore setUpStore() throws Exception {
        return LMDBPersistentStoreTest.createMapDBPersistentStoreForTest();
    }

    @Override
    protected void cleanUpStore() throws Exception {
        LMDBPersistentStoreTest.cleanUpMapDBStoreAfterTest();
    }

    public static LMDBPersistentStore createMapDBPersistentStoreForTest() throws Exception {
        IntegrationUtils.deleteDirectory(new File(BrokerConstants.DEFAULT_LMDB_STORE_PATH));
        Properties props = new Properties();
        props.setProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME, BrokerConstants.DEFAULT_LMDB_STORE_PATH);
        IConfig conf = new MemoryConfig(props);
        LMDBPersistentStore lmdbPersistentStore = new LMDBPersistentStore(conf);
        return lmdbPersistentStore;
    }

    public static void cleanUpMapDBStoreAfterTest() throws Exception {
        IntegrationUtils.deleteDirectory(new File(BrokerConstants.DEFAULT_LMDB_STORE_PATH));
    }

}

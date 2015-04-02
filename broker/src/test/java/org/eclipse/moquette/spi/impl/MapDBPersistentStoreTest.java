/*
 * Copyright (c) 2012-2014 The original author or authors
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
package org.eclipse.moquette.spi.impl;

import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.spi.impl.persistence.MapDBPersistentStore;
import org.eclipse.moquette.spi.impl.subscriptions.Subscription;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.eclipse.moquette.commons.Constants.DEFAULT_PERSISTENT_PATH;
import static org.junit.Assert.*;

/**
 *
 * @author andrea
 */
public class MapDBPersistentStoreTest {

    MapDBPersistentStore m_storageService;
        
    @Before
    public void setUp() throws Exception {
    	    	
        File dbFile = new File(DEFAULT_PERSISTENT_PATH);
        assertFalse(String.format("The DB storagefile %s already exists", DEFAULT_PERSISTENT_PATH), dbFile.exists());
        
        m_storageService = new MapDBPersistentStore(DEFAULT_PERSISTENT_PATH);
    }

    @After
    public void tearDown() {
        if (m_storageService != null) {
            m_storageService.close();
        }
        
        File dbFile = new File(DEFAULT_PERSISTENT_PATH);
        if (dbFile.exists()) {
        	assertTrue("Error deleting the moquette db file " + DEFAULT_PERSISTENT_PATH, dbFile.delete());
        }
        assertFalse(dbFile.exists());
    }

    @Test
    public void overridingSubscriptions() {
        Subscription oldSubscription = new Subscription("FAKE_CLI_ID_1", "/topic", AbstractMessage.QOSType.MOST_ONE, false);
        m_storageService.addNewSubscription(oldSubscription, oldSubscription.getClientId());
        Subscription overrindingSubscription = new Subscription("FAKE_CLI_ID_1", "/topic", AbstractMessage.QOSType.EXACTLY_ONCE, false);
        m_storageService.addNewSubscription(overrindingSubscription, overrindingSubscription.getClientId());
        
        //Verify
        List<Subscription> subscriptions = m_storageService.listAllSubscriptions();
        assertEquals(1, subscriptions.size());
        Subscription sub = subscriptions.get(0);
        assertEquals(overrindingSubscription.getRequestedQos(), sub.getRequestedQos());
    }
}
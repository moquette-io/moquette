package io.moquette.spi.persistence.lmdb;

import io.moquette.spi.impl.subscriptions.AbstractSubscriptionsStoreTest;

/**
*
* Test SubscriptionsStore using MapDB ISessionsStore implementation
*
* @author Jan Zajic
*/
public class LMDBSubscriptionsStoreTest extends AbstractSubscriptionsStoreTest {

    @Override
    protected LMDBPersistentStore setUpStore() throws Exception {
        return LMDBPersistentStoreTest.createMapDBPersistentStoreForTest();
    }

    @Override
    protected void cleanUpStore() throws Exception {
        LMDBPersistentStoreTest.cleanUpMapDBStoreAfterTest();
    }
    
}

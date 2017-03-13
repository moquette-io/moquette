package io.moquette.spi.persistence;

import io.moquette.spi.impl.subscriptions.AbstractSubscriptionsStoreTest;

/**
*
* Test SubscriptionsStore using MapDB ISessionsStore implementation
*
* @author Jan Zajic
*/
public class MapDBSubscriptionsStoreTest extends AbstractSubscriptionsStoreTest {

    @Override
    protected MapDBPersistentStore setUpStore() throws Exception {
        return MapDBPersistentStoreTest.createMapDBPersistentStoreForTest();
    }

    @Override
    protected void cleanUpStore() throws Exception {
        MapDBPersistentStoreTest.cleanUpMapDBStoreAfterTest();
    }
    
}

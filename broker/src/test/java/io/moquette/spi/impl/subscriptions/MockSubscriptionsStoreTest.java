package io.moquette.spi.impl.subscriptions;

import io.moquette.spi.impl.MemoryStorageService;
import io.moquette.spi.persistence.IPersistentStore;

/**
*
* Test SubscriptionsStore using mock ISessionsStore memory implementation
*
* @author Jan Zajic
*/
public class MockSubscriptionsStoreTest extends AbstractSubscriptionsStoreTest {

    @Override
    protected IPersistentStore setUpStore() throws Exception {
        MemoryStorageService storageService = new MemoryStorageService();
        return storageService;
    }

    @Override
    protected void cleanUpStore() throws Exception {
    }
    
}

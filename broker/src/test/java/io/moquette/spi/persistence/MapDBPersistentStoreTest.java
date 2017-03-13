package io.moquette.spi.persistence;

import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import io.moquette.BrokerConstants;
import io.moquette.server.IntegrationUtils;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;

public class MapDBPersistentStoreTest extends AbstractPersistentStoreTest {

    @Override
    protected MapDBPersistentStore setUpStore() throws Exception {
        return MapDBPersistentStoreTest.createMapDBPersistentStoreForTest();
    }

    @Override
    protected void cleanUpStore() throws Exception {
        MapDBPersistentStoreTest.cleanUpMapDBStoreAfterTest();
    }

    public static MapDBPersistentStore createMapDBPersistentStoreForTest() {
        IntegrationUtils.cleanPersistenceFile(BrokerConstants.DEFAULT_PERSISTENT_PATH);
        Properties props = new Properties();
        props.setProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME, BrokerConstants.DEFAULT_PERSISTENT_PATH);
        IConfig conf = new MemoryConfig(props);
        MapDBPersistentStore mapDBPersistentStore = new MapDBPersistentStore(conf);
        return mapDBPersistentStore;
    }
    
    public static void cleanUpMapDBStoreAfterTest() {
        IntegrationUtils.cleanPersistenceFile(BrokerConstants.DEFAULT_PERSISTENT_PATH);
    }
    
    @Test
    public void testCloseShutdownCommitTask() throws InterruptedException {
        m_storageService.close();
        MapDBPersistentStore mapDBPersistentStore = (MapDBPersistentStore) m_storageService;
        // verify the executor is shutdown
        assertTrue(
                "Storage service scheduler can't be stopped in 3 seconds",
                mapDBPersistentStore.m_scheduler.awaitTermination(3, TimeUnit.SECONDS));
        assertTrue(mapDBPersistentStore.m_scheduler.isTerminated());
    }
    
}

package io.moquette.spi.persistence;

import io.moquette.BrokerConstants;
import io.moquette.server.config.IConfig;
import io.moquette.spi.persistence.lmdb.LMDBPersistentStore;

public enum StoreType {

    MAPDB("mapdb", new IPersistentStoreFactory() {

        @Override
        public IPersistentStore createStore(IConfig props) {
            return new MapDBPersistentStore(props);
        }
        
    }), 
    LMDB("lmdb", new IPersistentStoreFactory() {

        @Override
        public IPersistentStore createStore(IConfig props) {
            return new LMDBPersistentStore(props);
        }
        
    });
    
    private StoreType(String configValue, IPersistentStoreFactory factory) {
        this.configValue = configValue;
        this.factory = factory;
    }
    
    private String configValue;
    private IPersistentStoreFactory factory;
        
    public IPersistentStoreFactory getFactory() {
        return factory;
    }
    
    public String getConfigValue() {
        return configValue;
    }
    
    public static StoreType createFromConfiguration(String configValue) {
        for (StoreType storeType : StoreType.values()) {
            if(storeType.configValue.equalsIgnoreCase(configValue))
                return storeType;
        }
        //MAPDB is default
        return StoreType.MAPDB; 
    }

    public static StoreType createFromConfiguration(IConfig props) {
        return createFromConfiguration(props.getProperty(BrokerConstants.STORE_TYPE_PROPERTY_NAME, BrokerConstants.STORE_TYPE_PROPERTY_DEFAULT));
    }
    
}

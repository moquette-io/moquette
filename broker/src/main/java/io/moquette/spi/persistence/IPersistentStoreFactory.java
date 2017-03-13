package io.moquette.spi.persistence;

import io.moquette.server.config.IConfig;

public interface IPersistentStoreFactory {

    IPersistentStore createStore(IConfig props);
    
}

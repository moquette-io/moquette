package io.moquette.spi.persistence;

import io.moquette.spi.IMessagesStore;
import io.moquette.spi.ISessionsStore;

public interface IPersistentStore {

    void initStore();

    IMessagesStore messagesStore();
    ISessionsStore sessionsStore();

    void close();

}

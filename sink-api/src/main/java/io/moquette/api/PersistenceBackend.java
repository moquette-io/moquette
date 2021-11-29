package io.moquette.api;

public interface PersistenceBackend {
    ISslContextCreator getSslContextCreator();
    IRetainedRepository getRetainedRepository();
    IQueueRepository getQueueRepository();
    ISubscriptionsRepository getSubscriptionsRepository();
}

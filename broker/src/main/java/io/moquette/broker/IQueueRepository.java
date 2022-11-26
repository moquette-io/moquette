package io.moquette.broker;

import java.util.Queue;
import java.util.Set;

public interface IQueueRepository {

    Set<String> listQueueNames();

    boolean containsQueue(String clientId);

    SessionMessageQueue<SessionRegistry.EnqueuedMessage> getOrCreateQueue(String clientId);
}

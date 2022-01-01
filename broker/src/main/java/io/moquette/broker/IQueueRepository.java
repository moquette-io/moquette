package io.moquette.broker;

import java.util.Map;
import java.util.Queue;

public interface IQueueRepository {

    Queue<SessionRegistry.EnqueuedMessage> createQueue(String cli, boolean clean);

    void removeQueue(String cli);

    Map<String, Queue<SessionRegistry.EnqueuedMessage>> listAllQueues();
}

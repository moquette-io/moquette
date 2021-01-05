package io.moquette.broker;

import java.util.Map;
import java.util.Queue;

public interface IQueueRepository {

    Queue<SessionRegistry.EnqueuedMessage> createQueue(String cli, boolean clean);

    Map<String, Queue<SessionRegistry.EnqueuedMessage>> listAllQueues();
}

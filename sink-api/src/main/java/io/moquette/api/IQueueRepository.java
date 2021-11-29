package io.moquette.api;

import java.util.Map;
import java.util.Queue;

public interface IQueueRepository {

    Queue<EnqueuedMessage> createQueue(String cli, boolean clean);

    Map<String, Queue<EnqueuedMessage>> listAllQueues();
}

package io.moquette.persistence;

import io.moquette.broker.IQueueRepository;
import io.moquette.broker.SessionRegistry.EnqueuedMessage;
import org.h2.mvstore.MVStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class H2QueueRepository implements IQueueRepository {

    private MVStore mvStore;

    public H2QueueRepository(MVStore mvStore) {
        this.mvStore = mvStore;
    }

    @Override
    public Queue<EnqueuedMessage> createQueue(String cli, boolean clean) {
        if (!clean) {
            return new H2PersistentQueue<>(mvStore, cli);
        }
        return new ConcurrentLinkedQueue<>();
    }

    @Override
    public Map<String, Queue<EnqueuedMessage>> listAllQueues() {
        Map<String, Queue<EnqueuedMessage>> result = new HashMap<>();
        mvStore.getMapNames().stream()
            .filter(name -> name.startsWith("queue_") && !name.endsWith("_meta"))
            .map(name -> name.substring("queue_".length()))
            .forEach(name -> result.put(name, new H2PersistentQueue<EnqueuedMessage>(mvStore, name)));
        return result;
    }
}

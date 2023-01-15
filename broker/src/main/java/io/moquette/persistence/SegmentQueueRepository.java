package io.moquette.persistence;

import io.moquette.broker.IQueueRepository;
import io.moquette.broker.SessionMessageQueue;
import io.moquette.broker.SessionRegistry;
import io.moquette.broker.unsafequeues.Queue;
import io.moquette.broker.unsafequeues.QueueException;
import io.moquette.broker.unsafequeues.QueuePool;

import java.nio.file.Paths;
import java.util.Set;

public class SegmentQueueRepository implements IQueueRepository {

    private final QueuePool queuePool;

    public SegmentQueueRepository(String path) throws QueueException {
        queuePool = QueuePool.loadQueues(Paths.get(path));
    }

    @Override
    public Set<String> listQueueNames() {
        return queuePool.queueNames();
    }

    @Override
    public boolean containsQueue(String clientId) {
        return listQueueNames().contains(clientId);
    }

    @Override
    public SessionMessageQueue<SessionRegistry.EnqueuedMessage> getOrCreateQueue(String clientId) {
        final Queue segmentedQueue;
        try {
            segmentedQueue = queuePool.getOrCreate(clientId);
        } catch (QueueException e) {
            throw new RuntimeException(e);
        }
        return new SegmentPersistentQueue(segmentedQueue);
    }
}

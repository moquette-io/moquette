package io.moquette.persistence;

import io.moquette.broker.IQueueRepository;
import io.moquette.broker.SessionMessageQueue;
import io.moquette.broker.SessionRegistry;
import io.moquette.broker.unsafequeues.Queue;
import io.moquette.broker.unsafequeues.QueueException;
import io.moquette.broker.unsafequeues.QueuePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

public class SegmentQueueRepository implements IQueueRepository {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentQueueRepository.class);

    private final QueuePool queuePool;

    public SegmentQueueRepository(String path, int pageSize, int segmentSize) throws QueueException {
        queuePool = QueuePool.loadQueues(Paths.get(path), pageSize, segmentSize);
    }

    public SegmentQueueRepository(Path path, int pageSize, int segmentSize) throws QueueException {
        queuePool = QueuePool.loadQueues(path, pageSize, segmentSize);
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

    @Override
    public void close() {
        try {
            queuePool.close();
        } catch (QueueException e) {
            LOG.error("Error saving state of the queue pool", e);
        }
    }
}

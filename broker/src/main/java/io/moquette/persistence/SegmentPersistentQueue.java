package io.moquette.persistence;

import io.moquette.broker.AbstractSessionMessageQueue;
import io.moquette.broker.SessionRegistry;
import io.moquette.broker.unsafequeues.Queue;
import io.moquette.broker.unsafequeues.QueueException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;

public class SegmentPersistentQueue extends AbstractSessionMessageQueue<SessionRegistry.EnqueuedMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentPersistentQueue.class);

    private final Queue segmentedQueue;
    private final SegmentedPersistentQueueSerDes serdes = new SegmentedPersistentQueueSerDes();

    public SegmentPersistentQueue(Queue segmentedQueue) {
        this.segmentedQueue = segmentedQueue;
    }

    @Override
    public void enqueue(SessionRegistry.EnqueuedMessage message) {
        LOG.debug("Adding message {}", message);
        checkEnqueuePreconditions(message);

        final ByteBuffer payload = serdes.toBytes(message);
        try {
            segmentedQueue.enqueue(payload);
        } catch (QueueException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SessionRegistry.EnqueuedMessage dequeue() {
        checkDequeuePreconditions();

        final Optional<ByteBuffer> dequeue;
        try {
            dequeue = segmentedQueue.dequeue();
        } catch (QueueException e) {
            throw new RuntimeException(e);
        }
        if (!dequeue.isPresent()) {
            LOG.debug("No data pulled out from the queue");
            return null;
        }

        final ByteBuffer content = dequeue.get();
        SessionRegistry.EnqueuedMessage message = serdes.fromBytes(content);
        LOG.debug("Retrieved message {}", message);
        return message;
    }

    @Override
    public boolean isEmpty() {
        return segmentedQueue.isEmpty();
    }

    @Override
    public void closeAndPurge() {
        closed = true;
    }
}

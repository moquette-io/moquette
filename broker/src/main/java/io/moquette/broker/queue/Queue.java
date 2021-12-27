package io.moquette.broker.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class Queue {
    private static final Logger LOG = LoggerFactory.getLogger(Queue.class);

    public static final int LENGTH_HEADER_SIZE = 4;
    private final String name;
    private final AtomicReference<Segment> headSegment;
    /* First writeable byte */
    private final AtomicReference<SegmentPointer> currentHeadPtr;
    private final SegmentAllocator allocator;
    private final PagedFilesAllocator.AllocationAction action;
    private final ReentrantLock lock = new ReentrantLock();

    Queue(String name, Segment headSegment, SegmentPointer currentHeadPtr, SegmentAllocator allocator, PagedFilesAllocator.AllocationAction action) {
        this.name = name;
        this.headSegment = new AtomicReference<>(headSegment);
        this.currentHeadPtr = new AtomicReference<>(currentHeadPtr);
        this.allocator = allocator;
        this.action = action;
    }

    /**
     * @throws QueueException if an error happens during access to file.
     * */
    public void enqueue(byte[] data) throws QueueException {
        final SegmentPointer res = spinningMove(LENGTH_HEADER_SIZE + data.length);
        if (res != null) {
            LOG.trace("CAT insertion at: {}", res);
            writeData(headSegment.get(), res, data);
            return;
        } else {
            lock.lock();
            long spaceNeeded;
            SegmentPointer lastOffset = null;

            // the bytes written from the data input
            long consumedBytes = 0;
            do {
                final Segment currentSegment = headSegment.get();
                spaceNeeded = currentSegment.bytesAfter(currentHeadPtr.get());
            } while (spaceNeeded != 0 && ((lastOffset = spinningMove(spaceNeeded)) == null));

            SegmentPointer newSegmentPointer = null;
            if (spaceNeeded != 0) {
                LOG.trace("Writing partial data to offset {} for {} bytes", lastOffset, spaceNeeded);
                final int copySize = (int) (spaceNeeded - LENGTH_HEADER_SIZE);
                byte[] copy = new byte[copySize];
                System.arraycopy(data, 0, copy, 0, copySize);
                writeData(headSegment.get(), lastOffset, data.length, copy);
                consumedBytes += copySize;
                newSegmentPointer = new SegmentPointer(headSegment.get(), currentHeadPtr.get().offset() + spaceNeeded);
            }

            Segment newSegment = null;
            try {
                while (consumedBytes < data.length) {
                    newSegment = allocator.nextFreeSegment();
                    //notify segment creation for queue in queue pool
                    action.segmentedCreated(name, newSegment);

                    int copySize = (int) Math.min(data.length - consumedBytes, Segment.SIZE);

                    byte[] copy = new byte[copySize];
                    System.arraycopy(data, (int) consumedBytes, copy, 0, copySize);
                    consumedBytes += copySize;
                    newSegmentPointer = new SegmentPointer(newSegment, newSegment.begin.offset() + (copySize + LENGTH_HEADER_SIZE) - 1);

                    // if not first write of data
                    if (consumedBytes > 0) {
                        writeDataNoHeader(newSegment, newSegment.begin, copy);
                    } else {
                        writeData(newSegment, newSegment.begin, copy);
                    }
                }

                // publish the last segment created and the pointer to head.
                if (newSegment != null) {
                    headSegment.set(newSegment);
                }
                if (newSegmentPointer != null) {
                    currentHeadPtr.set(newSegmentPointer);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void writeDataNoHeader(Segment segment, SegmentPointer start, byte[] data) {
        ByteBuffer content = ByteBuffer.allocate(data.length);
        content
            .put(data) // put the payload
            .flip();

        segment.write(start, content);
    }

    /**
     * Writes data and size to the current Head segment starting from start pointer.
     * */
    private void writeData(Segment segment, SegmentPointer start, byte[] data) {
        ByteBuffer content = ByteBuffer.allocate(LENGTH_HEADER_SIZE + data.length);
        content
            .putInt(data.length) // put 4 bytes header
            .put(data) // put the payload
            .flip();

        segment.write(start, content);
    }

    private void writeData(Segment segment, SegmentPointer start, int totalSize, byte[] data) {
        ByteBuffer content = ByteBuffer.allocate(LENGTH_HEADER_SIZE + data.length);
        content
            .putInt(totalSize) // put 4 bytes header
            .put(data) // put the payload
            .flip();

        segment.write(start, content);
    }

    /**
     * Move forward the currentHead pointer of size bytes, using CAS operation.
     * @return null if the head segment doesn't have enough space or the offset used as start of the move.
     * */
    private SegmentPointer spinningMove(long size) {
        SegmentPointer currentHeadPtr;
        SegmentPointer newHead;
        do {
            currentHeadPtr = this.currentHeadPtr.get();
            if (!headSegment.get().hasSpace(currentHeadPtr, size)) {
                return null;
            }
            newHead = currentHeadPtr.moveForward(size);
        } while (!this.currentHeadPtr.compareAndSet(currentHeadPtr, newHead));
        // the start position must the be the first free position, while the previous head reference
        // keeps the last occupied position, move .forward by 1
        return currentHeadPtr.plus(1);
    }

    /**
     * Used in test
     * */
    void force() {
        headSegment.get().force();
    }

    SegmentPointer currentHead() {
        return this.currentHeadPtr.get();
    }
}

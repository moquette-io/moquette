package io.moquette.broker.unsafequeues;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Not thread safe disk persisted queue.S
 * */
public class Queue {
    private static final Logger LOG = LoggerFactory.getLogger(Queue.class);

    public static final int LENGTH_HEADER_SIZE = 4;
    private final String name;
    /* Last wrote byte, point to head byte */
    private final VirtualPointer currentHeadPtr;
    private final Segment headSegment;

    /* First readable byte, point to the last occupied byte */
    private final AtomicReference<VirtualPointer> currentTailPtr;
    private final AtomicReference<Segment> tailSegment;

    private final QueuePool queuePool;
    private final PagedFilesAllocator.AllocationListener allocationListener;
    private final ReentrantLock lock = new ReentrantLock();

    Queue(String name, Segment headSegment, VirtualPointer currentHeadPtr,
          Segment tailSegment, VirtualPointer currentTailPtr,
          SegmentAllocator allocator, PagedFilesAllocator.AllocationListener allocationListener, QueuePool queuePool) {
        this.name = name;
        this.headSegment = headSegment;
        this.currentHeadPtr = currentHeadPtr;
        this.currentTailPtr = new AtomicReference<>(currentTailPtr);
        this.tailSegment = new AtomicReference<>(tailSegment);
        this.allocationListener = allocationListener;
        this.queuePool = queuePool;
    }

    /**
     * @throws QueueException if an error happens during access to file.
     * */
    public void enqueue(ByteBuffer payload) throws QueueException {
        final int messageSize = LENGTH_HEADER_SIZE + payload.remaining();
        if (headSegment.hasSpace(currentHeadPtr, messageSize)) {
            LOG.debug("Head segment has sufficient space for message length {}", LENGTH_HEADER_SIZE + payload.remaining();
            writeData(headSegment, currentHeadPtr, payload);
            // move head segment
            currentHeadPtr.moveForward(messageSize);
            return;
        }

        LOG.debug("Head segment doesn't have enough space");
        // the payload can't be fully contained into the current head segment and needs to be splitted
        // with another segment. To request the next segment, it's needed to be done in global lock.
        lock.lock();

        final int dataSize = payload.remaining();
        final ByteBuffer rawData = (ByteBuffer) ByteBuffer.allocate(LENGTH_HEADER_SIZE + dataSize)
            .putInt(dataSize)
            .put(payload)
            .flip();

        // the bytes written from the payload input
        long bytesRemainingInHeaderSegment = Math.min(rawData.remaining(), headSegment.bytesAfter(currentHeadPtr));
        LOG.trace("Writing partial payload to offset {} for {} bytes", currentHeadPtr, bytesRemainingInHeaderSegment);

        int copySize = (int) bytesRemainingInHeaderSegment;
        ByteBuffer slice = rawData.slice();
        slice.limit(copySize);
        writeDataNoHeader(headSegment, currentHeadPtr, slice);
        currentHeadPtr.moveForward(bytesRemainingInHeaderSegment);

        // No need to move newSegmentPointer the pointer because the last spinningMove has already moved it

        // shift forward the consumption point
        rawData.position(rawData.position() + copySize);

        Segment newSegment = null;
        try {
            // till the payload is not completely stored,
            // save the remaining part into a new segment.
            while (rawData.hasRemaining()) {
                newSegment = queuePool.nextFreeSegment();
                //notify segment creation for queue in queue pool
                allocationListener.segmentedCreated(name, newSegment);

                copySize = (int) Math.min(rawData.remaining(), Segment.SIZE);
                slice = rawData.slice();
                slice.limit(copySize);

                currentHeadPtr.moveForward(copySize);
                writeDataNoHeader(newSegment, newSegment.begin, slice);

                // shift forward the consumption point
                rawData.position(rawData.position() + copySize);
            }
        } finally {
            lock.unlock();
        }
    }

    private void writeDataNoHeader(Segment segment, SegmentPointer start, ByteBuffer data) {
        segment.write(start, data);
    }

    private void writeDataNoHeader(Segment segment, VirtualPointer start, ByteBuffer data) {
        segment.write(start, data);
    }

    /**
     * Writes data and size to the current Head segment starting from start pointer.
     * */
    private void writeData(Segment segment, VirtualPointer start, ByteBuffer data) {
        writeData(segment, start, data.remaining(), data);
    }

    /**
     * @param segment the target segment.
     * @param start where start writing.
     * @param size the length of the data to write on the segment.
     * @param data the data to write.
     * */
    private void writeData(Segment segment, VirtualPointer start, int size, ByteBuffer data) {
        ByteBuffer length = (ByteBuffer) ByteBuffer.allocate(LENGTH_HEADER_SIZE).putInt(size).flip();
        segment.write(start, length); // write 4 bytes header
        segment.write(start.plus(LENGTH_HEADER_SIZE), data); // write the payload
    }

    /**
     * Used in test
     * */
    void force() {
        headSegment.force();
    }

    VirtualPointer currentHead() {
        return currentHeadPtr;
    }

    VirtualPointer currentTail() {
        return this.currentTailPtr.get();
    }

    /**
     * Read next message or return null if the queue has no data.
     * */
    public ByteBuffer dequeue() throws QueueException {
        boolean retry;
        ByteBuffer out = null;
        do {
            retry = false;
            final Segment currentSegment = tailSegment.get();
            final VirtualPointer currentTail = currentTailPtr.get();
            if (head.get().pointer.isGreaterThan(currentTail)) {
                LOG.debug("currentTail is {}", currentTail);
                if (currentSegment.bytesAfter(currentTail) + 1 >= LENGTH_HEADER_SIZE) {
                    // currentSegment contains at least the header (payload length)
                    final VirtualPointer existingTail;
                    if (isTailFirstUsage(currentTail)) {
                        // move to the first readable byte
                        existingTail = currentTail.plus(1);
                    } else {
                        existingTail = currentTail.copy();
                    }
                    final int payloadLength = currentSegment.readHeader(existingTail);
                    //DBG breakpoint
                    if (payloadLength != 152433) {
                        LOG.debug("Hit header problem at segment {}, offset position {}, payloadLength {}",
                            currentSegment, currentSegment.rebasedOffset(existingTail), payloadLength);
                        currentSegment.readHeader(existingTail);
                        System.exit(1);
                    }
                    //DBG
                    // tail must be moved to the next byte to read, so has to move to
                    // header size + payload size + 1
                    final int fullMessageSize = payloadLength + LENGTH_HEADER_SIZE;
                    if (currentSegment.hasSpace(existingTail, fullMessageSize + 1)) {
                        // currentSegments contains fully the payload
                        final VirtualPointer newTail = existingTail.moveForward(fullMessageSize);
                        if (currentTailPtr.compareAndSet(currentTail, newTail)) {
                            LOG.debug("Moved currentTailPointer to {} from {} reading {} bytes", newTail, existingTail, fullMessageSize);
                            // fast track optimistic lock
                            // read data from currentTail + 4 bytes(the length)
                            final VirtualPointer dataStart = existingTail.moveForward(LENGTH_HEADER_SIZE);

                            out = readData(currentSegment, dataStart, payloadLength);
                        } else {
                            // some concurrent thread moved forward the tail pointer before us,
                            // retry with another message
                            retry = true;
                        }
                    } else {
                        // payload is split across currentSegment and next ones
                        lock.lock();
                        if (tailSegment.get().equals(currentSegment)) {
                            // tailSegment is still the currentSegment, and we are in the lock, so we own it, let's
                            // consume the segments
                            VirtualPointer dataStart = existingTail.moveForward(LENGTH_HEADER_SIZE);

                            LOG.debug("Loading payload size {}", payloadLength);
                            out = loadPayloadFromSegments(payloadLength, currentSegment, dataStart);
                        } else {
                            // tailSegments was moved in the meantime, this means some other thread
                            // has already consumed the data and theft the payload, go ahead with another message
                            retry = true;
                        }

                        lock.unlock();
                    }
                } else {
                    // header is split across 2 segments
                    lock.lock();
                    if (tailSegment.get().equals(currentSegment)) {
                        // the currentSegment is still the tailSegment
                        // read the length header that's crossing 2 segments
                        final CrossSegmentHeaderResult result = decodeCrossHeader(currentSegment, currentTail);

                        // load all payload parts from the segments
                        LOG.debug("Loading payload size {}", result.payloadLength);
                        out = loadPayloadFromSegments(result.payloadLength, result.segment, result.pointer);
                    } else {
                        // somebody else changed the tailSegment, retry and read next message
                        retry = true;
                    }
                    lock.unlock();
                }
            } else {
                if (currentTail.compareTo(head.get().pointer) == 0) {
                    // head and tail pointer are the same, the queue is empty
                    return null;
                }
                lock.lock();
                if (tailSegment.get().equals(currentSegment)) {
                    // load next tail segment
                    final Segment newTailSegment = queuePool.openNextTailSegment(name);

                    // assign to tailSegment without CAS because we are in lock
                    tailSegment.set(newTailSegment);
                }
                lock.unlock();
                retry = true;
            }
        } while (retry);

        // return data or null
        return out;
    }

    private static class CrossSegmentHeaderResult {
        private final Segment segment;
        private final VirtualPointer pointer;
        private final int payloadLength;

        private CrossSegmentHeaderResult(Segment segment, VirtualPointer pointer, int payloadLength) {
            this.segment = segment;
            this.pointer = pointer;
            this.payloadLength = payloadLength;
        }
    }

    // TO BE called owning the lock
    private CrossSegmentHeaderResult decodeCrossHeader(Segment segment, VirtualPointer pointer) throws QueueException {
        // read first part
        ByteBuffer lengthBuffer = ByteBuffer.allocate(LENGTH_HEADER_SIZE);
        final ByteBuffer partialHeader = segment.readAllBytesAfter(pointer);
        final int consumedHeaderSize = partialHeader.remaining();
        lengthBuffer.put(partialHeader);
        queuePool.consumedTailSegment(name);
        // DBG
        segment.fillWith((byte) 'D');
        // DBG

        // read second part
        final int remainingHeaderSize =  LENGTH_HEADER_SIZE - consumedHeaderSize;
        Segment nextTailSegment = queuePool.openNextTailSegment(name);
        lengthBuffer.put(nextTailSegment.read(nextTailSegment.begin, remainingHeaderSize));
        final VirtualPointer dataStart = pointer.moveForward(LENGTH_HEADER_SIZE);
        int payloadLength = ((ByteBuffer) lengthBuffer.flip()).getInt();

        return new CrossSegmentHeaderResult(nextTailSegment, dataStart, payloadLength);
    }

    // TO BE called owning the lock
    private ByteBuffer loadPayloadFromSegments(int remaining, Segment segment, VirtualPointer tail) throws QueueException {
        List<ByteBuffer> createdBuffers = new ArrayList<>(segmentCountFromSize(remaining));
        VirtualPointer scan = tail;

        do {
            LOG.debug("Looping remaining {}", remaining);
            int availableDataLength = Math.min(remaining, (int) segment.bytesAfter(scan) + 1);
            final ByteBuffer buffer = segment.read(scan, availableDataLength);
            createdBuffers.add(buffer);
            final boolean segmentCompletelyConsumed = (segment.bytesAfter(scan) + 1) == availableDataLength;
            scan = scan.moveForward(availableDataLength);
            final boolean consumedQueue = scan.isGreaterThan(currentHead());
            remaining -= buffer.remaining();

            if (remaining > 0 || (segmentCompletelyConsumed && !consumedQueue)) {
                queuePool.consumedTailSegment(name);
                // DBG
                segment.fillWith((byte) 'D');
                // DBG
                segment = queuePool.openNextTailSegment(name);
            }
        } while (remaining > 0);

        // assign to tailSegment without CAS because we are in lock
        tailSegment.set(segment);
        currentTailPtr.set(scan);
        LOG.debug("Moved currentTailPointer to {} from {}", scan, tail);

        return joinBuffers(createdBuffers);
    }

    private int segmentCountFromSize(int remaining) {
        return (int) Math.ceil((double) remaining / Segment.SIZE);
    }

    private boolean isTailFirstUsage(VirtualPointer tail) {
        return tail.isUntouched();
    }

    /**
     * @return a ByteBuffer that's a composition of all buffers
     * */
    private ByteBuffer joinBuffers(List<ByteBuffer> buffers) {
        final int neededSpace = buffers.stream().mapToInt(Buffer::remaining).sum();
        byte[] heapBuffer = new byte[neededSpace];
        int offset = 0;
        for (ByteBuffer buffer : buffers) {
            final int readBytes = buffer.remaining();
            buffer.get(heapBuffer, offset, readBytes);
            offset += readBytes;
        }

        return ByteBuffer.wrap(heapBuffer);
    }

    private ByteBuffer readData(Segment source, VirtualPointer start, int length) {
        return source.read(start, length);
    }
}

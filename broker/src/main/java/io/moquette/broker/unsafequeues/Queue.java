package io.moquette.broker.unsafequeues;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Not thread safe disk persisted queue.
 * */
public class Queue {
    private static final Logger LOG = LoggerFactory.getLogger(Queue.class);

    public static final int LENGTH_HEADER_SIZE = 4;
    private final String name;
    /* Last wrote byte, point to head byte */
    private VirtualPointer currentHeadPtr;
    private Segment headSegment;

    /* First readable byte, point to the last occupied byte */
    private VirtualPointer currentTailPtr;
    private Segment tailSegment;

    private final QueuePool queuePool;
    private final SegmentAllocator allocator;
    private final PagedFilesAllocator.AllocationListener allocationListener;
//    private final ReentrantLock lock = new ReentrantLock();

    Queue(String name, Segment headSegment, VirtualPointer currentHeadPtr,
          Segment tailSegment, VirtualPointer currentTailPtr,
          SegmentAllocator allocator, PagedFilesAllocator.AllocationListener allocationListener, QueuePool queuePool) {
        this.name = name;
        this.headSegment = headSegment;
        this.currentHeadPtr = currentHeadPtr;
        this.currentTailPtr = currentTailPtr;
        this.tailSegment = tailSegment;
        this.allocator = allocator;
        this.allocationListener = allocationListener;
        this.queuePool = queuePool;
    }

    /**
     * @throws QueueException if an error happens during access to file.
     * */
    public void enqueue(ByteBuffer payload) throws QueueException {
        final int messageSize = LENGTH_HEADER_SIZE + payload.remaining();
        if (headSegment.hasSpace(currentHeadPtr, messageSize)) {
            LOG.debug("Head segment has sufficient space for message length {}", LENGTH_HEADER_SIZE + payload.remaining());
            writeData(headSegment, currentHeadPtr.plus(1), payload);
            // move head segment
            currentHeadPtr = currentHeadPtr.moveForward(messageSize);
            return;
        }

        LOG.debug("Head segment doesn't have enough space");
        // the payload can't be fully contained into the current head segment and needs to be splitted
        // with another segment.


        final int dataSize = payload.remaining();
        final ByteBuffer rawData = (ByteBuffer) ByteBuffer.allocate(LENGTH_HEADER_SIZE + dataSize)
            .putInt(dataSize)
            .put(payload)
            .flip();

        // the bytes written from the payload input
        long bytesRemainingInHeaderSegment = Math.min(rawData.remaining(), headSegment.bytesAfter(currentHeadPtr));
        LOG.trace("Writing partial payload to offset {} for {} bytes", currentHeadPtr, bytesRemainingInHeaderSegment);

        if (bytesRemainingInHeaderSegment > 0) {
            int copySize = (int) bytesRemainingInHeaderSegment;
            ByteBuffer slice = rawData.slice();
            slice.limit(copySize);
            writeDataNoHeader(headSegment, currentHeadPtr.plus(1), slice);
            currentHeadPtr = currentHeadPtr.moveForward(bytesRemainingInHeaderSegment);
            // No need to move newSegmentPointer the pointer because the last spinningMove has already moved it

            // shift forward the consumption point
            rawData.position(rawData.position() + copySize);
        }

        Segment newSegment = null;

        // till the payload is not completely stored,
        // save the remaining part into a new segment.
        while (rawData.hasRemaining()) {
            // To request the next segment, it's needed to be done in global lock.
            newSegment = queuePool.nextFreeSegment();
            //notify segment creation for queue in queue pool
            allocationListener.segmentedCreated(name, newSegment);

            int copySize = (int) Math.min(rawData.remaining(), allocator.getSegmentSize());
            ByteBuffer slice = rawData.slice();
            slice.limit(copySize);

            currentHeadPtr = currentHeadPtr.moveForward(copySize);
            writeDataNoHeader(newSegment, newSegment.begin, slice);
            headSegment = newSegment;

            // shift forward the consumption point
            rawData.position(rawData.position() + copySize);
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
        return currentTailPtr;
    }

    public boolean isEmpty() {
        if (isTailFirstUsage(currentTailPtr)) {
            return currentHeadPtr.compareTo(currentTailPtr) == 0;
        } else {
            return currentHeadPtr.moveForward(1).compareTo(currentTailPtr) == 0;
        }
    }

    /**
     * Close the Queue and release all resources.
     */
    public void close() {
        queuePool.purgeQueue(name);
        headSegment = null;
        tailSegment = null;
    }

    /**
     * Read next message or return null if the queue has no data.
     * */
    public Optional<ByteBuffer> dequeue() throws QueueException {
        if (!currentHeadPtr.isGreaterThan(currentTailPtr)) {
            if (currentTailPtr.isGreaterThan(currentHeadPtr)) {
                // sanity check
                throw new QueueException("Current tail " + currentTailPtr + " is forward head " + currentHeadPtr);
            }
            // head and tail pointer are the same, the queue is empty
            return Optional.empty();
        }
        if (tailSegment == null) {
            tailSegment = queuePool.openNextTailSegment(name).get();
        }

        LOG.debug("currentTail is {}", currentTailPtr);
        if (containsHeader(tailSegment, currentTailPtr)) {
            // currentSegment contains at least the header (payload length)
            final VirtualPointer existingTail;
            if (isTailFirstUsage(currentTailPtr)) {
                // move to the first readable byte
                existingTail = currentTailPtr.plus(1);
            } else {
                existingTail = currentTailPtr.copy();
            }
            final int payloadLength = tailSegment.readHeader(existingTail);
            // tail must be moved to the next byte to read, so has to move to
            // header size + payload size + 1
            final int fullMessageSize = payloadLength + LENGTH_HEADER_SIZE;
            long remainingInSegment = tailSegment.bytesAfter(existingTail) + 1;
            if (remainingInSegment > fullMessageSize) {
                // tail segment fully contains the payload with space left over
                currentTailPtr = existingTail.moveForward(fullMessageSize);
                // read data from currentTail + 4 bytes(the length)
                final VirtualPointer dataStart = existingTail.moveForward(LENGTH_HEADER_SIZE);

                return Optional.of(readData(tailSegment, dataStart, payloadLength));
            } else {
                // payload is split across currentSegment and next ones
                VirtualPointer dataStart = existingTail.moveForward(LENGTH_HEADER_SIZE);

                if (remainingInSegment - LENGTH_HEADER_SIZE == 0) {
                    queuePool.consumedTailSegment(name);
                    if (QueuePool.queueDebug) {
                        tailSegment.fillWith((byte) 'D');
                    }
                    tailSegment = queuePool.openNextTailSegment(name).get();
                }

                LOG.debug("Loading payload size {}", payloadLength);
                return Optional.of(loadPayloadFromSegments(payloadLength, tailSegment, dataStart));
            }
        } else {
            // header is split across 2 segments
            // the currentSegment is still the tailSegment
            // read the length header that's crossing 2 segments
            final CrossSegmentHeaderResult result = decodeCrossHeader(tailSegment, currentTailPtr);

            // load all payload parts from the segments
            LOG.debug("Loading payload size {}", result.payloadLength);
            return Optional.of(loadPayloadFromSegments(result.payloadLength, result.segment, result.pointer));
        }
    }

    private static boolean containsHeader(Segment segment, VirtualPointer tail) {
        return segment.bytesAfter(tail) + 1 >= LENGTH_HEADER_SIZE;
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

        if (QueuePool.queueDebug) {
            segment.fillWith((byte) 'D');
        }

        // read second part
        final int remainingHeaderSize = LENGTH_HEADER_SIZE - consumedHeaderSize;
        Segment nextTailSegment = queuePool.openNextTailSegment(name).get();
        lengthBuffer.put(nextTailSegment.read(nextTailSegment.begin, remainingHeaderSize));
        final VirtualPointer dataStart = pointer.moveForward(LENGTH_HEADER_SIZE);
        int payloadLength = ((ByteBuffer) lengthBuffer.flip()).getInt();

        return new CrossSegmentHeaderResult(nextTailSegment, dataStart, payloadLength);
    }

    // TO BE called owning the lock on segments allocator
    private ByteBuffer loadPayloadFromSegments(int remaining, Segment segment, VirtualPointer tail) throws QueueException {
        List<ByteBuffer> createdBuffers = new ArrayList<>(segmentCountFromSize(remaining));
        VirtualPointer scan = tail;

        do {
            LOG.debug("Looping remaining {}", remaining);
            final int availableDataLength = Math.min(remaining, (int) segment.bytesAfter(scan) + 1);
            final ByteBuffer buffer = segment.read(scan, availableDataLength);
            createdBuffers.add(buffer);
            final boolean segmentCompletelyConsumed = (segment.bytesAfter(scan) + 1) == availableDataLength;
            scan = scan.moveForward(availableDataLength);
            remaining -= buffer.remaining();

            if (remaining > 0 || segmentCompletelyConsumed) {
                queuePool.consumedTailSegment(name);
                if (QueuePool.queueDebug) {
                    segment.fillWith((byte) 'D');
                }
                segment = queuePool.openNextTailSegment(name).orElse(null);
            }
        } while (remaining > 0);

        // assign to tailSegment without CAS because we are in lock
        tailSegment = segment;
        currentTailPtr = scan;
        LOG.debug("Moved currentTailPointer to {} from {}", scan, tail);

        return joinBuffers(createdBuffers);
    }

    private int segmentCountFromSize(int remaining) {
        return (int) Math.ceil((double) remaining / allocator.getSegmentSize());
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

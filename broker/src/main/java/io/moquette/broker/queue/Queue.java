package io.moquette.broker.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class Queue {
    private static final Logger LOG = LoggerFactory.getLogger(Queue.class);

    public static final int LENGTH_HEADER_SIZE = 4;
    private final String name;
    private final AtomicReference<Segment> headSegment;
    /* First writeable byte, point to first free byte */
    private final AtomicReference<SegmentPointer> currentHeadPtr;
    /* First readable byte, point to the last occupied byte */
    private final AtomicReference<SegmentPointer> currentTailPtr;
    private final AtomicReference<Segment> tailSegment;

    private final SegmentAllocator allocator;
    private final QueuePool queuePool;
    private final PagedFilesAllocator.AllocationListener allocationListener;
    private final ReentrantLock lock = new ReentrantLock();

    Queue(String name, Segment headSegment, SegmentPointer currentHeadPtr,
          Segment tailSegment, SegmentPointer currentTailPtr,
          SegmentAllocator allocator, PagedFilesAllocator.AllocationListener allocationListener, QueuePool queuePool) {
        this.name = name;
        this.headSegment = new AtomicReference<>(headSegment);
        this.currentHeadPtr = new AtomicReference<>(currentHeadPtr);
        this.currentTailPtr = new AtomicReference<>(currentTailPtr);
        this.tailSegment = new AtomicReference<>(tailSegment);
        this.allocator = allocator;
        this.allocationListener = allocationListener;
        this.queuePool = queuePool;
    }

    /**
     * @throws QueueException if an error happens during access to file.
     * */
    public void enqueue(ByteBuffer payload) throws QueueException {
        final SegmentPointer res = spinningMove(LENGTH_HEADER_SIZE + payload.remaining());
        if (res != null) {
            // in this case all the payload is contained in the current head segment
            LOG.trace("CAS insertion at: {}", res);
            writeData(headSegment.get(), res, payload);
            return;
        } else {
            // the payload can't be fully contained into the current head segment and needs to be splitted
            // with another segment. To request the next segment, it's needed to be done in global lock.
            lock.lock();

            final int dataSize = payload.remaining();
            final ByteBuffer rawData = (ByteBuffer) ByteBuffer.allocate(LENGTH_HEADER_SIZE + dataSize)
                .putInt(dataSize)
                .put(payload)
                .flip();

            // the bytes written from the payload input
            long bytesRemainingInHeaderSegment;
            SegmentPointer lastOffset = null;
            do {
                // there could be another thread that's pushing data to the segment
                // outside the lock, so conquer with that to grab the remaining space
                // in the segment.
                final Segment currentSegment = headSegment.get();
                bytesRemainingInHeaderSegment = currentSegment.bytesAfter(currentHeadPtr.get());
            } while (bytesRemainingInHeaderSegment != 0 && ((lastOffset = spinningMove(bytesRemainingInHeaderSegment)) == null));

            SegmentPointer newSegmentPointer = null;
            if (bytesRemainingInHeaderSegment != 0) {
                // copy the beginning part of payload into the head segment, to fill it up.
                LOG.trace("Writing partial payload to offset {} for {} bytes", lastOffset, bytesRemainingInHeaderSegment);

                final int copySize = (int) bytesRemainingInHeaderSegment;
                final ByteBuffer slice = rawData.slice();
                slice.limit(copySize);
                writeDataNoHeader(headSegment.get(), lastOffset, slice);

                newSegmentPointer = new SegmentPointer(headSegment.get(), currentHeadPtr.get().offset() + bytesRemainingInHeaderSegment);

                // shift forward the consumption point
                rawData.position(rawData.position() + copySize);
            }

            Segment newSegment = null;
            try {
                // till the payload is not completely stored,
                // save the remaining part into a new segment.
                while (rawData.hasRemaining()) {
                    newSegment = allocator.nextFreeSegment();
                    //notify segment creation for queue in queue pool
                    allocationListener.segmentedCreated(name, newSegment);

                    int copySize = (int) Math.min(rawData.remaining(), Segment.SIZE);
                    final ByteBuffer slice = rawData.slice();
                    slice.limit(copySize);

                    newSegmentPointer = new SegmentPointer(newSegment, newSegment.begin.offset() + copySize - 1);
                    writeDataNoHeader(newSegment, newSegment.begin, slice);

                    // shift forward the consumption point
                    rawData.position(rawData.position() + copySize);
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

    private void writeDataNoHeader(Segment segment, SegmentPointer start, ByteBuffer data) {
        segment.write(start, data);
    }

    /**
     * Writes data and size to the current Head segment starting from start pointer.
     * */
    private void writeData(Segment segment, SegmentPointer start, ByteBuffer data) {
        writeData(segment, start, data.remaining(), data);
    }

    /**
     * @param segment the target segment.
     * @param start where start writing.
     * @param size the length of the data to write on the segment.
     * @param data the data to write.
     * */
    private void writeData(Segment segment, SegmentPointer start, int size, ByteBuffer data) {
        ByteBuffer length = (ByteBuffer) ByteBuffer.allocate(LENGTH_HEADER_SIZE).putInt(size).flip();
        segment.write(start, length); // write 4 bytes header
        segment.write(start.plus(LENGTH_HEADER_SIZE), data); // write the payload
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
        // the start position must be the first free position, while the previous head reference
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

    SegmentPointer currentTail() {
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
            final SegmentPointer currentTail = currentTailPtr.get();
            if (hasData(currentHeadPtr.get(), currentTail)) {
                if (currentSegment.bytesAfter(currentTail) >= LENGTH_HEADER_SIZE) {
                    // header of message (the length) is contained in the current segment
                    SegmentPointer existingTail = new SegmentPointer(currentTail);
                    if (isTailFirstUsage(currentSegment, currentTail)) {
                        existingTail = currentTail.plus(1);
                    }
                    int messageLength = currentSegment.readHeader(existingTail);
                    if (currentSegment.hasSpace(existingTail, messageLength + LENGTH_HEADER_SIZE)) {
                        final SegmentPointer newTail = existingTail.moveForward(messageLength + LENGTH_HEADER_SIZE);
                        if (currentTailPtr.compareAndSet(currentTail, newTail)) {
                            // fast track optimistic lock
                            // read data from currentTail + 4 bytes(the length)
                            out = readData(currentSegment, existingTail.moveForward(LENGTH_HEADER_SIZE), messageLength);
                        } else {
                            retry = true;
                        }
                    } else {
                        lock.lock();
                        // ~1
                        if (tailSegment.get().equals(currentSegment)) {
                            // consume the segment
                            messageLength = currentSegment.readHeader(existingTail);
                            int remaining = messageLength;
                            SegmentPointer dataStart = existingTail.moveForward(LENGTH_HEADER_SIZE);
                            // WARN, dataStart point to a byte position to read
                            // if currentSegment.end is 1023 offset, and data start is 1020, the bytes after are 4 and
                            // not 1023 - 1020.
                            final long tmpLen = currentSegment.bytesAfter(dataStart) + 1;
                            List<ByteBuffer> createdBuffers = new ArrayList<>();
                            createdBuffers.add(readData(currentSegment, dataStart, (int) tmpLen));

                            queuePool.consumedTailSegment(name);

                            remaining -= tmpLen;
                            Segment newTailSegment;
                            SegmentPointer lastTail;
                            do {
                                // load next tail segment
                                newTailSegment = queuePool.openNextTailSegment(name);
                                dataStart = newTailSegment.begin;
                                int toRead = Math.min(remaining, (int) Segment.SIZE);
                                final ByteBuffer readMessagePart = readData(newTailSegment, dataStart, toRead);
                                createdBuffers.add(readMessagePart);
                                remaining -= toRead;
                                lastTail = dataStart.moveForward(toRead);
                                if (remaining > 0) {
                                    queuePool.consumedTailSegment(name);
                                }
                            } while (remaining > 0);

                            out = joinBuffers(createdBuffers);

                            // assign to tailSegment without CAS because we are in lock
                            tailSegment.set(newTailSegment);
                            currentTailPtr.set(lastTail);
                        } else {
                            retry = true;
                        }

                        lock.unlock();
                    }
                } else {
                    lock.lock();
                    // ~1
                    if (tailSegment.get().equals(currentSegment)) {
                        // read the length header that's crossing 2 segments
                        ByteBuffer lengthBuffer = ByteBuffer.allocate(LENGTH_HEADER_SIZE);
                        final int remainingHeader = (int) currentSegment.bytesAfter(currentTail) + 1;
                        lengthBuffer.put(currentSegment.read(currentTail, remainingHeader));

                        queuePool.consumedTailSegment(name);

                        final int headerFragmentToRead =  LENGTH_HEADER_SIZE - remainingHeader;
                        Segment newTailSegment = queuePool.openNextTailSegment(name);
                        lengthBuffer.put(newTailSegment.read(newTailSegment.begin, headerFragmentToRead));
                        final SegmentPointer dataStart = newTailSegment.begin.moveForward(headerFragmentToRead);

                        final int messageLength = ((ByteBuffer) lengthBuffer.flip()).getInt();
                        int remaining = messageLength;
                        int toRead = Math.min(remaining, (int) Segment.SIZE);
                        List<ByteBuffer> createdBuffers = new ArrayList<>();
                        createdBuffers.add(newTailSegment.read(dataStart, toRead));

                        SegmentPointer lastTail = dataStart.moveForward(toRead);

                        remaining -= toRead;

                        while (remaining > 0) {
                            newTailSegment = queuePool.openNextTailSegment(name);
                            toRead = Math.min(remaining, (int) Segment.SIZE);
                            createdBuffers.add(newTailSegment.read(newTailSegment.begin, toRead));
                            lastTail = newTailSegment.begin.moveForward(toRead);
                            remaining -= toRead;
                            if (remaining > 0) {
                                queuePool.consumedTailSegment(name);
                            }
                        }

                        out = joinBuffers(createdBuffers);

                        // assign to tailSegment without CAS because we are in lock
                        tailSegment.set(newTailSegment);
                        currentTailPtr.set(lastTail);
                    } else {
                        retry = true;
                    }
                    lock.unlock();
                }
            } else {
                //TODO the first run tail is (0, 0) while head is (0, -1)
                if (currentTail.compareTo(currentHeadPtr.get()) == 0) {
                    return null;
                }
                lock.lock();
                // 1
                if (tailSegment.get().equals(currentSegment)) {
                    // load next tail segment
                    final Segment newTailSegment = queuePool.openNextTailSegment(name);

                    // assign to tailSegment without CAS because we are in lock
                    tailSegment.set(newTailSegment);
                    currentTailPtr.set(newTailSegment.begin);
                }
                lock.unlock();
                retry = true;
            }
        } while (retry);

        // return data or null
        return out;
    }

    private boolean isTailFirstUsage(Segment tailSegment, SegmentPointer tail) {
        return tail.equals(tailSegment.begin.plus(-1));
    }

    private boolean hasData(SegmentPointer head, SegmentPointer tail) {
        return head.compareTo(tail) > 0;
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

    private ByteBuffer readData(Segment source, SegmentPointer start, int length) {
        return source.read(start, length);
    }
}

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
    /* First writeable byte */
    private final AtomicReference<SegmentPointer> currentHeadPtr;
    /* First readable byte */
    private final AtomicReference<SegmentPointer> currentTailPtr;
    private final AtomicReference<Segment> tailSegment;

    private final SegmentAllocator allocator;
    private final QueuePool queuePool;
    private final PagedFilesAllocator.AllocationAction action;
    private final ReentrantLock lock = new ReentrantLock();

    Queue(String name, Segment headSegment, SegmentPointer currentHeadPtr,
          Segment tailSegment, SegmentPointer currentTailPtr,
          SegmentAllocator allocator, PagedFilesAllocator.AllocationAction action, QueuePool queuePool) {
        this.name = name;
        this.headSegment = new AtomicReference<>(headSegment);
        this.currentHeadPtr = new AtomicReference<>(currentHeadPtr);
        this.currentTailPtr = new AtomicReference<>(currentTailPtr);
        this.tailSegment = new AtomicReference<>(tailSegment);
        this.allocator = allocator;
        this.action = action;
        this.queuePool = queuePool;
    }

    /**
     * @throws QueueException if an error happens during access to file.
     * */
    public void enqueue(ByteBuffer data) throws QueueException {
        final SegmentPointer res = spinningMove(LENGTH_HEADER_SIZE + data.remaining());
        if (res != null) {
            LOG.trace("CAS insertion at: {}", res);
            writeData(headSegment.get(), res, data);
            return;
        } else {
            lock.lock();
            long spaceNeeded;
            SegmentPointer lastOffset = null;

            // the bytes written from the data input
            do {
                final Segment currentSegment = headSegment.get();
                spaceNeeded = currentSegment.bytesAfter(currentHeadPtr.get());
            } while (spaceNeeded != 0 && ((lastOffset = spinningMove(spaceNeeded)) == null));

            SegmentPointer newSegmentPointer = null;
            boolean firstWrite = true;
            if (spaceNeeded != 0) {
                LOG.trace("Writing partial data to offset {} for {} bytes", lastOffset, spaceNeeded);
                final int dataSize = data.remaining();

                final int copySize = (int) (spaceNeeded - LENGTH_HEADER_SIZE);
                final ByteBuffer slice = data.slice();
                slice.limit(copySize);

                writeData(headSegment.get(), lastOffset, dataSize, slice);
                firstWrite = false;
                newSegmentPointer = new SegmentPointer(headSegment.get(), currentHeadPtr.get().offset() + spaceNeeded);

                // shift forward the consumption point
                data.position(data.position() + copySize);
            }

            Segment newSegment = null;
            try {
                while (data.hasRemaining()) {
                    newSegment = allocator.nextFreeSegment();
                    //notify segment creation for queue in queue pool
                    action.segmentedCreated(name, newSegment);

                    int copySize = (int) Math.min(data.remaining(), Segment.SIZE);
                    final ByteBuffer slice = data.slice();
                    slice.limit(copySize);

                    newSegmentPointer = new SegmentPointer(newSegment, newSegment.begin.offset() + (copySize + LENGTH_HEADER_SIZE) - 1);

                    // if not first write of data
                    if (!firstWrite) {
                        writeDataNoHeader(newSegment, newSegment.begin, slice);
                    } else {
                        writeData(newSegment, newSegment.begin, slice);
                    }
                    firstWrite = false;

                    // shift forward the consumption point
                    data.position(data.position() + copySize);
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
                        final int remainingHeader = (int) currentSegment.bytesAfter(currentTail);
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

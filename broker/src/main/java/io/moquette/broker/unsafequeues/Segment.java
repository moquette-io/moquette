package io.moquette.broker.unsafequeues;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

final class Segment {
    private static final Logger LOG = LoggerFactory.getLogger(Segment.class);

    final int segmentSize;
    final SegmentPointer begin;
    final SegmentPointer end;

    private final MappedByteBuffer mappedBuffer;

    Segment(MappedByteBuffer page, SegmentPointer begin, SegmentPointer end) {
        assert begin.samePage(end);
        this.segmentSize = end.offset() - begin.offset() + 1;
        this.begin = begin;
        this.end = end;
        this.mappedBuffer = page;
    }

    boolean hasSpace(VirtualPointer mark, long length) {
        return bytesAfter(mark) >= length;
    }

    /**
     * @return number of bytes in segment after the pointer.
     * The pointer slot is not counted.
     * */
    public long bytesAfter(SegmentPointer mark) {
        assert mark.samePage(this.end);
        return end.distance(mark);
    }

    public long bytesAfter(VirtualPointer mark) {
        final int pageOffset = rebasedOffset(mark);
        final SegmentPointer physicalMark = new SegmentPointer(this.end.pageId(), pageOffset);
        return end.distance(physicalMark);
    }

    void write(SegmentPointer offset, ByteBuffer content) {
        checkContentStartWith(content);
        final int startPos = offset.offset();
        final int endPos = startPos + content.remaining();
        for (int i = startPos; i < endPos; i++) {
            mappedBuffer.put(i, content.get());
        }
    }

    // fill the segment with value bytes
    void fillWith(byte value) {
        LOG.debug("Wipe segment {}", this);
        final int target = begin.offset() + (int)size();
        for (int i = begin.offset(); i < target; i++) {
            mappedBuffer.put(i, value);
        }
    }

    // debug method
    private void checkContentStartWith(ByteBuffer content) {
        if (content.get(0) == 0 && content.get(1) == 0 && content.get(2) == 0 && content.get(3) == 0) {
            System.out.println("DNADBG content starts with 4 zero");
        }
    }

    void write(VirtualPointer offset, ByteBuffer content) {
        final int startPos = rebasedOffset(offset);
        final int endPos = startPos + content.remaining();
        for (int i = startPos; i < endPos; i++) {
            mappedBuffer.put(i, content.get());
        }
    }

    /**
     * Force flush of memory mapper buffer to disk
     * */
    void force() {
        mappedBuffer.force();
    }

    /**
     * return the int value contained in the 4 bytes after the pointer.
     *
     * @param pointer virtual pointer to start read from.
     * */
    int readHeader(VirtualPointer pointer) {
        final int rebasedIndex = rebasedOffset(pointer);
        LOG.debug(" {} {} {} {} at {}", Integer.toHexString(mappedBuffer.get(rebasedIndex)),
            Integer.toHexString(mappedBuffer.get(rebasedIndex + 1)),
            Integer.toHexString(mappedBuffer.get(rebasedIndex + 2)),
            Integer.toHexString(mappedBuffer.get(rebasedIndex + 3)),
            pointer
        );
        return mappedBuffer.getInt(rebasedIndex);
    }

    /*private*/ int rebasedOffset(VirtualPointer virtualPtr) {
        final int pointerOffset = (int) virtualPtr.segmentOffset(segmentSize);
        return this.begin.plus(pointerOffset).offset();
    }

    public ByteBuffer read(VirtualPointer start, int length) {
        final int pageOffset = rebasedOffset(start);
        byte[] dst = new byte[length];

        int sourceIdx = pageOffset;
        for (int dstIndex = 0; dstIndex < length; dstIndex++, sourceIdx++) {
            dst[dstIndex] = mappedBuffer.get(sourceIdx);
        }

        return ByteBuffer.wrap(dst);
    }

    public ByteBuffer read(SegmentPointer start, int length) {
        byte[] dst = new byte[length];

        if (length > mappedBuffer.remaining() - start.offset())
            throw new BufferUnderflowException();

        int sourceIdx = start.offset();
        for (int dstIndex = 0; dstIndex < length; dstIndex++, sourceIdx++) {
            dst[dstIndex] = mappedBuffer.get(sourceIdx);
        }

        return ByteBuffer.wrap(dst);
    }

    private long size() {
        return end.distance(begin) + 1;
    }

    @Override
    public String toString() {
        return "Segment{page=" + begin.pageId() + ", begin=" + begin.offset() + ", end=" + end.offset() + ", size=" + size() + "}";
    }

    ByteBuffer readAllBytesAfter(SegmentPointer start) {
        // WARN, dataStart points to a byte position to read
        // if currentSegment.end is at offset 1023, and data start is 1020, the bytes after are 4 and
        // not 1023 - 1020.
        final long availableDataLength = bytesAfter(start) + 1;
        final ByteBuffer buffer = read(start, (int) availableDataLength);
        buffer.rewind();
        return buffer;
    }

    ByteBuffer readAllBytesAfter(VirtualPointer start) {
        // WARN, dataStart points to a byte position to read
        // if currentSegment.end is at offset 1023, and data start is 1020, the bytes after are 4 and
        // not 1023 - 1020.
        final long availableDataLength = bytesAfter(start) + 1;
        final ByteBuffer buffer = read(start, (int) availableDataLength);
        buffer.rewind();
        return buffer;
    }
}

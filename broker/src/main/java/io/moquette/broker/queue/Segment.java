package io.moquette.broker.queue;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

final class Segment {
    final static long SIZE = 4 * 1024 * 1024;

    final SegmentPointer begin;
    final SegmentPointer end;

    private final MappedByteBuffer mappedBuffer;

    Segment(MappedByteBuffer page, SegmentPointer begin, SegmentPointer end) {
        assert begin.samePage(end);
        this.begin = begin;
        this.end = end;
        this.mappedBuffer = page;
    }

    boolean hasSpace(SegmentPointer mark, long length) {
        return mark.samePage(this.end) &&
            mark.moveForward(length).compareTo(this.end) <= 0;
    }

    /**
     * @return number of bytes in segment after the pointer.
     * The pointer slot is not counted.
     * */
    public long bytesAfter(SegmentPointer mark) {
        assert mark.samePage(this.end);
        return end.distance(mark);
    }

    void write(SegmentPointer offset, ByteBuffer content) {
        final ByteBuffer buffer = (ByteBuffer) mappedBuffer.position(offset.offset());
        buffer.put(content);
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
     * @param pointer*/
    int readHeader(SegmentPointer pointer) {
        return mappedBuffer.getInt(pointer.offset());
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

    @Override
    public String toString() {
        return "Segment{page=" + begin.pageId() + ", begin=" + begin.offset() + ", end=" + end.offset() + "}";
    }
}

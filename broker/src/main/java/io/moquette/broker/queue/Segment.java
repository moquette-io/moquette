package io.moquette.broker.queue;

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
// TODO            <= 0 > mark has t0 be <= end
            mark.moveForward(length).compareTo(this.end) <= 0;
    }

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
}

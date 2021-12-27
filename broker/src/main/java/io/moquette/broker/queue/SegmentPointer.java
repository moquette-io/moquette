package io.moquette.broker.queue;

final class SegmentPointer implements Comparable<SegmentPointer> {
    private final int idPage;
    private final long offset;

    public SegmentPointer(int idPage, long offset) {
        this.idPage = idPage;
        this.offset = offset;
    }

    /**
     * Construct using the segment, but changing the offset.
     * */
    public SegmentPointer(Segment segment, long offset) {
        this.idPage = segment.begin.idPage;
        this.offset = offset;
    }

    @Override
    public int compareTo(SegmentPointer other) {
        if (idPage == other.idPage) {
            return Long.compare(offset, other.offset);
        } else {
            return Integer.compare(idPage, other.idPage);
        }
    }

    boolean samePage(SegmentPointer other) {
        return idPage == other.idPage;
    }

    SegmentPointer moveForward(long length) {
        return new SegmentPointer(idPage, offset + length);
    }

    @Override
    public String toString() {
        return "SegmentPointer{idPage=" + idPage + ", offset=" + offset + '}';
    }

    /**
     * Calculate the distance in bytes inside the same segment
     * */
    public long distance(SegmentPointer other) {
        assert idPage == other.idPage;
        return offset - other.offset;
    }

    int offset() {
        return (int) offset;
    }

    public SegmentPointer plus(int delta) {
        return new SegmentPointer(this.idPage, offset + delta);
    }

    int pageId() {
        return this.idPage;
    }
}

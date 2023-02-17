package io.moquette.broker.unsafequeues;

public class VirtualPointer implements Comparable<VirtualPointer> {
    final long logicalOffset;

    public static VirtualPointer buildUntouched() {
        return new VirtualPointer(-1);
    }

    public VirtualPointer(long logicalOffset) {
        this.logicalOffset = logicalOffset;
    }

    @Override
    public int compareTo(VirtualPointer other) {
        return Long.compare(logicalOffset, other.logicalOffset);
    }

    public long segmentOffset(int segmentSize) {
        return logicalOffset % segmentSize;
    }

    public long logicalOffset() {
        return logicalOffset;
    }

    public VirtualPointer moveForward(long delta) {
        return new VirtualPointer(logicalOffset + delta);
    }

    public VirtualPointer plus(int i) {
        return new VirtualPointer(logicalOffset + i);
    }

    public boolean isGreaterThan(VirtualPointer other) {
        return this.compareTo(other) > 0;
    }

    public boolean isUntouched() {
        return logicalOffset == -1;
    }

    public VirtualPointer copy() {
        return new VirtualPointer(this.logicalOffset);
    }

    @Override
    public String toString() {
        return "VirtualPointer{logicalOffset=" + logicalOffset + '}';
    }
}

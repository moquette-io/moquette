package io.moquette.broker.unsafequeues;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SegmentPointerTest {

    @Test
    public void testCompareInSameSegment() {
        final SegmentPointer minor = new SegmentPointer(1, 10);
        final SegmentPointer otherMinor = new SegmentPointer(1, 10);
        final SegmentPointer major = new SegmentPointer(1, 12);

        assertEquals(-1, minor.compareTo(major), "minor is less than major");
        assertEquals(1, major.compareTo(minor), "major is greater than minor");
        assertEquals(0, minor.compareTo(otherMinor), "minor equals itself");
    }

    @Test
    public void testCompareInDifferentSegments() {
        final SegmentPointer minor = new SegmentPointer(1, 10);
        final SegmentPointer otherMinor = new SegmentPointer(1, 10);
        final SegmentPointer major = new SegmentPointer(2, 4);

        assertEquals(-1, minor.compareTo(major), "minor is less than major");
        assertEquals(1, major.compareTo(minor), "major is greater than minor");
        assertEquals(0, minor.compareTo(otherMinor), "minor equals itself");
    }
}

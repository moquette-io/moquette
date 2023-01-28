package io.moquette.broker.unsafequeues;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SegmentTest {

    @Test
    public void testHasSpace() throws IOException {
        final MappedByteBuffer pageBuffer = Utils.createPageFile();

        final Segment segment = new Segment(pageBuffer, new SegmentPointer(0, 0), new SegmentPointer(0, 1023));

        final VirtualPointer current = new VirtualPointer(511);
        final VirtualPointer otherCurrent = current.moveForward(pageBuffer.capacity()); // pointer in next page

        assertTrue(segment.hasSpace(current, 512));
        assertFalse(segment.hasSpace(current, 513));
        assertFalse(segment.hasSpace(otherCurrent, 513));
    }

    @Test
    public void testBytesAfter() throws IOException {
        final MappedByteBuffer pageBuffer = Utils.createPageFile();

        final SegmentPointer begin = new SegmentPointer(0, 0);
        final SegmentPointer end = new SegmentPointer(0, 1023);
        final Segment segment = new Segment(pageBuffer, begin, end);

        assertEquals(0, segment.bytesAfter(end));
        assertEquals(1023, segment.bytesAfter(begin));
    }
}

package io.moquette.broker.queue;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class SegmentTest {

    @Test
    public void testHasSpace() throws IOException {
        final MappedByteBuffer pageBuffer = Utils.createPageFile();

        final Segment segment = new Segment(pageBuffer, new SegmentPointer(0, 0), new SegmentPointer(0, 1023));

        final SegmentPointer current = new SegmentPointer(0, 511);
        final SegmentPointer otherCurrent = new SegmentPointer(1, 511);

        assertTrue(segment.hasSpace(current, 512));
        assertFalse(segment.hasSpace(current, 513));
        assertFalse(segment.hasSpace(otherCurrent, 513));
    }
}

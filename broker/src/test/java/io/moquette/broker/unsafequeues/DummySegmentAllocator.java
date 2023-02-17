package io.moquette.broker.unsafequeues;

import io.moquette.BrokerConstants;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Properties;

public class DummySegmentAllocator implements SegmentAllocator {

    @Override
    public Segment nextFreeSegment() {
        final MappedByteBuffer pageBuffer = createFreshPageTmpTile();
        final SegmentPointer begin = new SegmentPointer(0, 0);
        final SegmentPointer end = new SegmentPointer(0, BrokerConstants.DEFAULT_SEGMENTED_QUEUE_SEGMENT_SIZE);
        return new Segment(pageBuffer, begin, end);
    }

    @Override
    public Segment reopenSegment(int pageId, int beginOffset) throws QueueException {
        final MappedByteBuffer pageBuffer = createFreshPageTmpTile();
        final SegmentPointer begin = new SegmentPointer(pageId, beginOffset);
        final SegmentPointer end = new SegmentPointer(pageId, beginOffset + BrokerConstants.DEFAULT_SEGMENTED_QUEUE_SEGMENT_SIZE);
        return new Segment(pageBuffer, begin, end);
    }

    private MappedByteBuffer createFreshPageTmpTile() {
        final MappedByteBuffer pageBuffer;
        try {
            pageBuffer = Utils.createPageFile();
        } catch (IOException ex) {
            // used only in tests, so it's safe to fail the test with an untyped exception
            throw new RuntimeException(ex);
        }
        return pageBuffer;
    }

    @Override
    public void close() throws QueueException {
        // TODO, maybe
    }

    @Override
    public void dumpState(Properties checkpoint) {
    }

    @Override
    public int getPageSize() {
        return BrokerConstants.DEFAULT_SEGMENTED_QUEUE_PAGE_SIZE;
    }

    @Override
    public int getSegmentSize() {
        return BrokerConstants.DEFAULT_SEGMENTED_QUEUE_SEGMENT_SIZE;
    }
}

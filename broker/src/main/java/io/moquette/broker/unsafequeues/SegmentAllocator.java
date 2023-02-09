package io.moquette.broker.unsafequeues;

import java.util.Properties;

interface SegmentAllocator {

    /**
     * Return the next free segment in the current page, or create a new Page if necessary.
     *
     * This method has to be invoked inside a lock, it's not thread safe.
     *
     * @throws QueueException if any IO error happens on the filesystem.
     * */
    Segment nextFreeSegment() throws QueueException;

    Segment reopenSegment(int pageId, int beginOffset) throws QueueException;

    void close() throws QueueException;

    void dumpState(Properties checkpoint);

    /**
     * Get the size of a page that this allocator uses.
     *
     * @return the size of a page that this allocator uses.
     */
    int getPageSize();

    /**
     * Get the size of a segment that this allocator uses.
     *
     * @return the size of a segment that this allocator uses.
     */
    int getSegmentSize();
}

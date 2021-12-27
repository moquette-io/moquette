package io.moquette.broker.queue;


import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

/**
 * Default implementation of SegmentAllocator. It uses a series of files (named pages) and split them in segments.
 *
 * This class is not thread safe.
 * */
class PagedFilesAllocator implements SegmentAllocator {

    interface AllocationAction {
        void segmentedCreated(String name, Segment segment);
    }

    public static final int MB = 1024 * 1024;
    public static final int PAGE_SIZE = 64 * MB;
    private final Path pagesFolder;
    private final int segmentSize;
    private int lastSegmentAllocated;
    private int lastPage;
    private MappedByteBuffer currentPage;
    private FileChannel currentPageFile;

    PagedFilesAllocator(Path pagesFolder, int segmentSize, int lastPage, int lastSegmentAllocated) throws QueueException {
        this.pagesFolder = pagesFolder;
        this.segmentSize = segmentSize;
        this.lastPage = lastPage;
        this.lastSegmentAllocated = lastSegmentAllocated;
        this.currentPage = openRWPageFile(this.pagesFolder, this.lastPage);
    }

    private MappedByteBuffer openRWPageFile(Path pagesFolder, int pageId) throws QueueException {
        final Path pageFile = pagesFolder.resolve(String.format("%d.page", pageId));
        if (!Files.exists(pageFile)) {
            try {
                pageFile.toFile().createNewFile();
            } catch (IOException ex) {
                throw new QueueException("Reached an IO error during the bootstrapping of empty 'checkpoint.properties'", ex);
            }
        }

        final OpenOption[] openOptions = {StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        try (FileChannel fileChannel = FileChannel.open(pageFile, openOptions)) {
            this.currentPageFile = fileChannel;
            return fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
        } catch (IOException e) {
            throw new QueueException("Can't open page file " + pageFile, e);
        }
    }

    @Override
    public Segment nextFreeSegment() throws QueueException {
        if (currentPageIsExhausted()) {
            lastPage ++;
            currentPage = openRWPageFile(pagesFolder, lastPage);
            lastSegmentAllocated = 0;
        }

        final int beginOffset = lastSegmentAllocated * segmentSize;
        final int endOffset = ((lastSegmentAllocated + 1) * segmentSize) - 1;

        lastSegmentAllocated += 1;
        return new Segment(currentPage, new SegmentPointer(lastPage, beginOffset), new SegmentPointer(lastPage, endOffset));
    }

    @Override
    public Segment reopenSegment(int pageId, int beginOffset) throws QueueException {
        final MappedByteBuffer page = openRWPageFile(pagesFolder, pageId);
        final SegmentPointer begin = new SegmentPointer(pageId, beginOffset);
        final SegmentPointer end = new SegmentPointer(pageId, beginOffset + segmentSize);
        return new Segment(page, begin, end);
    }

    @Override
    public void close() throws QueueException {
        if (currentPageFile != null) {
            try {
                currentPageFile.close();
            } catch (IOException ex) {
                throw new QueueException("Problem closing current page file", ex);
            }
        }
    }

    @Override
    public void dumpState(Properties checkpoint) {
        checkpoint.setProperty("segments.last_page", String.valueOf(this.lastPage));
        checkpoint.setProperty("segments.last_segment", String.valueOf(this.lastSegmentAllocated));
    }

    private boolean currentPageIsExhausted() {
        return lastSegmentAllocated * segmentSize == PAGE_SIZE;
    }
}

package io.moquette.broker.unsafequeues;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class QueuePool {

    private static final Logger LOG = LoggerFactory.getLogger(QueuePool.class);

    static final boolean queueDebug = Boolean.parseBoolean(System.getProperty("moquette.queue.debug", "false"));

    private final SegmentAllocationCallback callback;

    // visible for testing
    static class SegmentRef implements Comparable<SegmentRef> {
        final int pageId;
        final int offset;

        // visible for testing
        SegmentRef(int pageId, int offset) {
            this.pageId = pageId;
            this.offset = offset;
        }

        public SegmentRef(Segment segment) {
            this.pageId = segment.begin.pageId();
            this.offset = segment.begin.offset();
        }

        @Override
        public String toString() {
            return String.format("(%d, %d)", pageId, offset);
        }

        @Override
        public int compareTo(SegmentRef o) {
            final int pageCompare = Integer.compare(pageId, o.pageId);
            if (pageCompare != 0) {
                return pageCompare;
            }
            return Integer.compare(offset, o.offset);
        }
    }

    private static class QueueName {
        final String name;

        private QueueName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueueName queueName = (QueueName) o;
            return Objects.equals(name, queueName.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return "QueueName{name='" + name + '\'' + '}';
        }
    }

    private final SegmentAllocator allocator;
    private final Path dataPath;
    private final int segmentSize;
    private final ConcurrentMap<QueueName, LinkedList<SegmentRef>> queueSegments = new ConcurrentHashMap<>();
    private final ConcurrentMap<QueueName, Queue> queues = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<SegmentRef> recycledSegments = new ConcurrentSkipListSet<>();
    private final ReentrantLock segmentsAllocationLock = new ReentrantLock();

    private QueuePool(SegmentAllocator allocator, Path dataPath, int segmentSize) {
        this.allocator = allocator;
        this.dataPath = dataPath;
        this.segmentSize = segmentSize;
        this.callback = new SegmentAllocationCallback(this);
    }

    private static class SegmentAllocationCallback implements PagedFilesAllocator.AllocationListener {

        private final QueuePool queuePool;

        private SegmentAllocationCallback(QueuePool queuePool) {
            this.queuePool = queuePool;
        }

        @Override
        public void segmentedCreated(String name, Segment segment) {
            queuePool.segmentedCreated(name, segment);
        }
    }

    private void segmentedCreated(String name, Segment segment) {
        LOG.debug("Registering new segment {} for queue {}", segment, name);
        final QueueName queueName = new QueueName(name);
        List<SegmentRef> segmentRefs = this.queueSegments.computeIfAbsent(queueName, k -> new LinkedList<>());

        // adds in head
        segmentRefs.add(0, new SegmentRef(segment));

        LOG.debug("queueSegments for queue {} after insertion {}", queueName, segmentRefs);
    }

    public static QueuePool loadQueues(Path dataPath, int pageSize, int segmentSize) throws QueueException {
        // read in checkpoint.properties
        final Properties checkpointProps = createOrLoadCheckpointFile(dataPath);

        // load last references to segment and instantiate the allocator
        final int lastPage = Integer.parseInt(checkpointProps.getProperty("segments.last_page", "0"));
        final int lastSegment = Integer.parseInt(checkpointProps.getProperty("segments.last_segment", "0"));

        final PagedFilesAllocator allocator = new PagedFilesAllocator(dataPath, pageSize, segmentSize, lastPage, lastSegment);

        final QueuePool queuePool = new QueuePool(allocator, dataPath, segmentSize);
        queuePool.loadQueueDefinitions(checkpointProps);
        LOG.debug("Loaded queues definitions: {}", queuePool.queueSegments);

        queuePool.loadRecycledSegments(checkpointProps);
        LOG.debug("Recyclable segments are: {}", queuePool.recycledSegments);
        return queuePool;
    }

    public Set<String> queueNames() {
        return queues.keySet().stream().map(qn -> qn.name).collect(Collectors.toSet());
    }

    private static Properties createOrLoadCheckpointFile(Path dataPath) throws QueueException {
        final Path checkpointPath = dataPath.resolve("checkpoint.properties");
        if (!Files.exists(checkpointPath)) {
            LOG.info("Can't find any file named 'checkpoint.properties' in path: {}, creating new one", dataPath);
            final boolean notExisted;
            try {
                notExisted = checkpointPath.toFile().createNewFile();
            } catch (IOException e) {
                LOG.error("IO Error creating the file {}", checkpointPath, e);
                throw new QueueException("Reached an IO error during the bootstrapping of empty 'checkpoint.properties'", e);
            }
            if (!notExisted) {
                LOG.warn("Found a checkpoint file while bootstrapping {}", checkpointPath);
            }
        }

        final FileReader fileReader;
        try {
            fileReader = new FileReader(checkpointPath.toFile());
        } catch (FileNotFoundException e) {
            throw new QueueException("Can't find any file named 'checkpoint.properties' in path: " + dataPath, e);
        }
        final Properties checkpointProps = new Properties();
        try {
            checkpointProps.load(fileReader);
        } catch (IOException e) {
            throw new QueueException("if an error occurred when reading from: " + checkpointPath, e);
        }
        return checkpointProps;
    }

    private void loadQueueDefinitions(Properties checkpointProps) throws QueueException {
        // structure of queues definitions in properties file:
        // queues.0.name = bla bla
        // queues.0.segments = head (id_page, offset), (id_page, offset), ... tail
        // queues.0.head_offset = bytes offset from the start of the page where last data was written
        // queues.0.tail_offset = bytes offset from the start of the page where first data could be read
        boolean noMoreQueues = false;
        int queueId = 0;
        while (!noMoreQueues) {
            final String queueKey = String.format("queues.%d.name", queueId);
            if (!checkpointProps.containsKey(queueKey)) {
                noMoreQueues = true;
                continue;
            }
            final QueueName queueName = new QueueName(checkpointProps.getProperty(queueKey));
            LinkedList<SegmentRef> segmentRefs = decodeSegments(checkpointProps.getProperty(String.format("queues.%d.segments", queueId)));
            final int numSegments = segmentRefs.size();
            queueSegments.put(queueName, segmentRefs);

            final long headOffset = Long.parseLong(checkpointProps.getProperty(String.format("queues.%d.head_offset", queueId)));
            final SegmentRef headSegmentRef = segmentRefs.get(0);
            final SegmentPointer currentHead = new SegmentPointer(headSegmentRef.pageId, headOffset);
            // TODO this reopen could be done in lazy way during getOrCreate method.
            Segment headSegment = allocator.reopenSegment(headSegmentRef.pageId, headSegmentRef.offset);

            final long tailOffset = Long.parseLong(checkpointProps.getProperty(String.format("queues.%d.tail_offset", queueId)));
            final SegmentRef tailSegmentRef = segmentRefs.getLast();
            final SegmentPointer currentTail = new SegmentPointer(tailSegmentRef.pageId, tailOffset);
            Segment tailSegment = allocator.reopenSegment(tailSegmentRef.pageId, tailSegmentRef.offset);

            // Create relative positioned head and tail pointers
            // Tail is an offset relative to start of the first segment in the list
            // Head is n-1 full segments plus the offset of the physical head
            final VirtualPointer logicalTail = new VirtualPointer(currentTail.offset());
            final VirtualPointer logicalHead = new VirtualPointer((long) (numSegments - 1) * segmentSize + currentHead.offset());
            final Queue queue = new Queue(queueName.name, headSegment, logicalHead, tailSegment, logicalTail,
                allocator, callback, this);
            queues.put(queueName, queue);

            queueId++;
        }
    }

    private void loadRecycledSegments(Properties checkpointProps) throws QueueException {
        TreeSet<SegmentRef> usedSegments = new TreeSet<>();

        boolean noMoreQueues = false;
        int queueId = 0;

        // load all queues definitions from checkpoint file
        // TODO second use of this, extract as an iterator
        while (!noMoreQueues) {
            final String queueKey = String.format("queues.%d.name", queueId);
            if (!checkpointProps.containsKey(queueKey)) {
                noMoreQueues = true;
                continue;
            }
            LinkedList<SegmentRef> segmentRefs = decodeSegments(checkpointProps.getProperty(String.format("queues.%d.segments", queueId)));
            usedSegments.addAll(segmentRefs);

            queueId++;
        }

        if (usedSegments.isEmpty()) {
            // no queue definitions were loaded
            return;
        }

        final List<SegmentRef> recreatedSegments = recreateSegmentHoles(usedSegments);

        segmentsAllocationLock.lock();
        try {
            recycledSegments.addAll(recreatedSegments);
        } finally {
            segmentsAllocationLock.unlock();
        }
    }

    /**
     * @param usedSegments sorted set of used segments
     * */
    // package-private for testing
    List<SegmentRef> recreateSegmentHoles(TreeSet<SegmentRef> usedSegments) throws QueueException {
        // find the holes in the list of used segments
        if (usedSegments.isEmpty()) {
            throw new QueueException("Status error, expected to find at least one segment");
        }

        // prev point to the last examined segment.
        // recreates segments on left of current segment.
        SegmentRef prev = null;
        final List<SegmentRef> recreatedSegments = new LinkedList<>();
        for (SegmentRef current : usedSegments) {
            if (prev == null) {
                // recreate recycled segments before first used segment
                recreatedSegments.addAll(recreateRecycledSegmentsBetween(current));
                prev = current;
                continue;
            }
            if (isAdjacent(prev, current)) {
                // contiguous, skip it
                prev = current;
                continue;
            }
            if (prev.pageId == current.pageId) {
                recreatedSegments.addAll(recreateRecycledSegments(prev.offset + segmentSize, current.offset, prev.pageId));
            } else {
                // recreate recycled segments between 2 used segments
                recreatedSegments.addAll(recreateRecycledSegmentsBetween(prev, current));
            }
        }
        return recreatedSegments;
    }

    private boolean isAdjacent(SegmentRef prev, SegmentRef segment) {
        if (prev.pageId == segment.pageId) {
            // same page
            if (prev.offset + segmentSize == segment.offset) {
                // contiguous, skip it
                return true;
            }
        } else if (prev.pageId + 1 == segment.pageId) {
            // adjacent pages, last segment in one and first in the other
            if (prev.offset == allocator.getPageSize() - segmentSize && segment.offset == 0) {
                return true;
            }
        }
        return false;
    }

    private List<SegmentRef> recreateRecycledSegmentsBetween(SegmentRef toSegment) {
        return recreateRecycledSegmentsBetween(null, toSegment);
    }

    private List<SegmentRef> recreateRecycledSegmentsBetween(SegmentRef fromSegment, SegmentRef toSegment) {
        final List<SegmentRef> recreatedSegments = new LinkedList<>();
        int prevPageId = 0;
        if (fromSegment != null) {
            prevPageId = fromSegment.pageId;
            // holes after previous segment, to complete the page
            recreatedSegments.addAll(recreateRecycledSegments(fromSegment.offset + segmentSize, allocator.getPageSize(), fromSegment.pageId));
            prevPageId++;
        }

        // all the intermediate pages
        for (; prevPageId < toSegment.pageId; prevPageId++) {
            recreatedSegments.addAll(recreateRecycledSegments(0, allocator.getPageSize(), prevPageId));
        }

        // holes before the current segment
        recreatedSegments.addAll(recreateRecycledSegments(0, toSegment.offset, toSegment.pageId));
        return recreatedSegments;
    }

    private List<SegmentRef> recreateRecycledSegments(int fromOffset, int toOffset, int pageId) {
        final List<SegmentRef> recreatedSegments = new LinkedList<>();
        while (fromOffset != toOffset) {
            recreatedSegments.add(new SegmentRef(pageId, fromOffset));
            fromOffset = fromOffset + segmentSize;
        }
        return recreatedSegments;
    }

    private LinkedList<SegmentRef> decodeSegments(String s) {
        final String[] segments = s.substring(s.indexOf("(") + 1, s.lastIndexOf(")"))
                .split("\\), \\(");

        LinkedList<SegmentRef> acc = new LinkedList<>();
        for (String segment : segments) {
            final String[] split = segment.split(",");
            final int idPage = Integer.parseInt(split[0].trim());
            final int offset = Integer.parseInt(split[1].trim());

            acc.offer(new SegmentRef(idPage, offset));
        }
        return acc;
    }

    public Queue getOrCreate(String queueName) throws QueueException {
        final QueueName queueN = new QueueName(queueName);
        if (queues.containsKey(queueN)) {
            return queues.get(queueN);
        } else {
            // create new queue with first empty segment
            final Segment segment = nextFreeSegment();
            //notify segment creation for queue in queue pool
            segmentedCreated(queueName, segment);

            // When a segment is freshly created the head must the last occupied byte,
            // so can't be the start of a segment, but one position before, or in case
            // of a new page, -1
            final Queue queue = new Queue(queueName, segment, VirtualPointer.buildUntouched(), segment, VirtualPointer.buildUntouched(),
                this.allocator, callback, this);
            queues.put(queueN, queue);
            return queue;
        }
    }

    void purgeQueue(String name) {
        final QueueName queueName = new QueueName(name);
        final LinkedList<SegmentRef> segmentRefs = queueSegments.remove(queueName);
        SegmentRef segmentRef = segmentRefs.pollLast();
        segmentsAllocationLock.lock();
        LOG.debug("Purging segments for queue {}", queueName);
        try {
            while (segmentRef != null) {
                LOG.debug("Purging segment {} from queue {}", segmentRef, queueName);
                recycledSegments.add(segmentRef);
                segmentRef = segmentRefs.pollLast();
            }
        } finally {
            segmentsAllocationLock.unlock();
        }
        queues.remove(queueName);
    }

    /**
     * Free mapped files
     * */
    public void close() throws QueueException {
        allocator.close();

        //save all into the checkpoint file
        Properties checkpoint = new Properties();
        allocator.dumpState(checkpoint);

        int queueCounter = 0;
        for (Map.Entry<QueueName, LinkedList<SegmentRef>> entry : queueSegments.entrySet()) {
            // queues.0.name = bla bla
            final QueueName queueName = entry.getKey();
            checkpoint.setProperty("queues." + queueCounter + ".name", queueName.name);

            // queues.0.segments = head (id_page, offset), (id_page, offset), ... tail
            final LinkedList<SegmentRef> segmentRefs = entry.getValue();
            final String segmentsDef = segmentRefs.stream()
                .map(SegmentRef::toString)
                .collect(Collectors.joining(", "));
            checkpoint.setProperty("queues." + queueCounter + ".segments", segmentsDef);

            // queues.0.head_offset = bytes offset from the start of the page where last data was written
            final Queue queue = queues.get(queueName);
            checkpoint.setProperty("queues." + queueCounter + ".head_offset", String.valueOf(queue.currentHead().segmentOffset(segmentSize)));
            checkpoint.setProperty("queues." + queueCounter + ".tail_offset", String.valueOf(queue.currentTail().segmentOffset(segmentSize)));
        }

        final File propertiesFile = dataPath.resolve("checkpoint.properties").toFile();
        final FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(propertiesFile);
        } catch (IOException ex) {
            throw new QueueException("Problem opening checkpoint.properties file", ex);
        }
        try {
            checkpoint.store(fileWriter, "DON'T EDIT, AUTOGENERATED");
        } catch (IOException ex) {
            throw new QueueException("Problem writing checkpoint.properties file", ex);
        }
    }

    Optional<Segment> openNextTailSegment(String name) throws QueueException {
        // definition from QueuePool.queueSegments
        final QueueName queueName = new QueueName(name);
        final LinkedList<SegmentRef> segmentRefs = queueSegments.get(queueName);

        final SegmentRef pollSegment = segmentRefs.peekLast();
        if (pollSegment == null) {
            return Optional.empty();
        }

        final Path pageFile = dataPath.resolve(String.format("%d.page", pollSegment.pageId));
        if (!Files.exists(pageFile)) {
            throw new QueueException("Can't find file for page file" + pageFile);
        }

        final MappedByteBuffer tailPage;
        try (FileChannel fileChannel = FileChannel.open(pageFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            tailPage = fileChannel.map(FileChannel.MapMode.READ_WRITE/*READ_ONLY*/, 0, allocator.getPageSize());
        } catch (IOException ex) {
            throw new QueueException("Can't open page file " + pageFile, ex);
        }

        final SegmentPointer begin = new SegmentPointer(pollSegment.pageId, pollSegment.offset);
        final SegmentPointer end = new SegmentPointer(pollSegment.pageId, pollSegment.offset + segmentSize - 1);
        return Optional.of(new Segment(tailPage, begin, end));
    }

    /**
     * Notify the actual tail segment was completely read
     * */
    void consumedTailSegment(String name) {
        final QueueName queueName = new QueueName(name);
        final LinkedList<SegmentRef> segmentRefs = queueSegments.get(queueName);
        final SegmentRef segmentRef = segmentRefs.pollLast();
        LOG.debug("Consumed tail segment {} from queue {}", segmentRef, queueName);
        segmentsAllocationLock.lock();
        try {
            recycledSegments.add(segmentRef);
        } finally {
            segmentsAllocationLock.unlock();
        }
    }

    Segment nextFreeSegment() throws QueueException {
        segmentsAllocationLock.lock();
        try {
            if (recycledSegments.isEmpty()) {
                LOG.debug("no recycled segments available, request the creation of new one");
                return allocator.nextFreeSegment();
            }
            final SegmentRef recycledSegment = recycledSegments.pollFirst();
            if (recycledSegment == null) {
                throw new QueueException("Invalid state, expected available recycled segment");
            }
            LOG.debug("Reusing recycled segment from page: {} at page offset: {}", recycledSegment.pageId, recycledSegment.offset);
            return allocator.reopenSegment(recycledSegment.pageId, recycledSegment.offset);
        } finally {
            segmentsAllocationLock.unlock();
        }
    }
}

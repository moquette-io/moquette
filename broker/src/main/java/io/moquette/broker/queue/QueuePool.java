package io.moquette.broker.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class QueuePool {

    private static final Logger LOG = LoggerFactory.getLogger(QueuePool.class);
    private static SegmentAllocationCallback callback;

    private static class SegmentRef {
        final int pageId;
        final int offset;

        private SegmentRef(int pageId, int offset) {
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
    }

    private final SegmentAllocator allocator;
    private final Path dataPath;
    private final Map<QueueName, List<SegmentRef>> queueSegments = new HashMap<>();
    private final Map<QueueName, Queue> queues = new HashMap<>();

    private QueuePool(SegmentAllocator allocator, Path dataPath) {
        this.allocator = allocator;
        this.dataPath = dataPath;
    }

    private static class SegmentAllocationCallback implements PagedFilesAllocator.AllocationAction {

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
        final QueueName queueName = new QueueName(name);
        List<SegmentRef> segmentRefs = this.queueSegments.computeIfAbsent(queueName, k -> new ArrayList<>());

        segmentRefs.add(new SegmentRef(segment));
    }

    public static QueuePool loadQueues(Path dataPath) throws QueueException {
        // read in checkpoint.properties
        final Properties checkpointProps = createOrLoadCheckpointFile(dataPath);

        // load last references to segments and instantiate the allocator
        final int lastPage = Integer.parseInt(checkpointProps.getProperty("segments.last_page", "0"));
        final int lastSegment = Integer.parseInt(checkpointProps.getProperty("segments.last_segment", "0"));

        final PagedFilesAllocator allocator = new PagedFilesAllocator(dataPath, (int) Segment.SIZE, lastPage, lastSegment);

        final QueuePool queuePool = new QueuePool(allocator, dataPath);
        callback = new SegmentAllocationCallback(queuePool);
        queuePool.loadQueueDefinitions(checkpointProps);
        return queuePool;
    }

    private static Properties createOrLoadCheckpointFile(Path dataPath) throws QueueException {
        final Path checkpointPath = dataPath.resolve("checkpoint.properties");
        if (!Files.exists(checkpointPath)) {
            LOG.info("Can't find any file named 'checkpoint.properties' in path: {}, creating new one", dataPath);
            final boolean notExisted;
            try {
                notExisted = checkpointPath.toFile().createNewFile();
            } catch (IOException e) {
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
        boolean noMoreQueues = false;
        int queueId = 0;
        while (!noMoreQueues) {
            final String queueKey = String.format("queues.%d.name", queueId);
            if (!checkpointProps.containsKey(queueKey)) {
                noMoreQueues = true;
                continue;
            }
            final QueueName queueName = new QueueName(checkpointProps.getProperty(queueKey));
            List<SegmentRef> segmentRefs = decodeSegments(checkpointProps.getProperty(String.format("queues.%d.segments", queueId)));
            queueSegments.put(queueName, segmentRefs);

            final long headOffset = Long.parseLong(checkpointProps.getProperty(String.format("queues.%d.head_offset", queueId)));

            final SegmentRef headSegmentRef = segmentRefs.get(0);
            final SegmentPointer currentHead = new SegmentPointer(headSegmentRef.pageId, headOffset);
            // TODO this reopen could be done in lazy way during getOrCreate method.
            Segment headSegment = allocator.reopenSegment(headSegmentRef.pageId, headSegmentRef.offset);

            final Queue queue = new Queue(queueName.name, headSegment, currentHead, allocator, callback);
            queues.put(queueName, queue);

            queueId++;
        }
    }

    private List<SegmentRef> decodeSegments(String s) {
        final String[] segments = s.substring(s.indexOf("(") + 1, s.lastIndexOf(")"))
                .split("\\), \\(");

        List<SegmentRef> acc = new ArrayList<>();
        for (String segment : segments) {
            final String[] split = segment.split(",");
            final int idPage = Integer.parseInt(split[0].trim());
            final int offset = Integer.parseInt(split[1].trim());

            acc.add(new SegmentRef(idPage, offset));
        }
        return acc;
    }

    public Queue getOrCreate(String queueName) throws QueueException {
        final QueueName queueN = new QueueName(queueName);
        if (queues.containsKey(queueN)) {
            return queues.get(queueN);
        } else {
            // create new queue with first empty segment
            final Segment segment = allocator.nextFreeSegment();
            //notify segment creation for queue in queue pool
            segmentedCreated(queueName, segment);

            // When a segment is freshly created the head must the last occupied byte,
            // so can't be the begin of a segment, but one position before, or in case
            // of a new page, -1
            final Queue queue = new Queue(queueName, segment, segment.begin.plus(-1), this.allocator, callback);
            queues.put(queueN, queue);
            return queue;
        }
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
        for (Map.Entry<QueueName, List<SegmentRef>> entry : queueSegments.entrySet()) {
            // queues.0.name = bla bla
            final QueueName queueName = entry.getKey();
            checkpoint.setProperty("queues." + queueCounter + ".name", queueName.name);

            // queues.0.segments = head (id_page, offset), (id_page, offset), ... tail
            final List<SegmentRef> segmentRefs = entry.getValue();
            final String segmentsDef = segmentRefs.stream()
                .map(SegmentRef::toString)
                .collect(Collectors.joining(", "));
            checkpoint.setProperty("queues." + queueCounter + ".segments", segmentsDef);

            // queues.0.head_offset = bytes offset from the start of the page where last data was written
            final Queue queue = queues.get(queueName);
            checkpoint.setProperty("queues." + queueCounter + ".head_offset", String.valueOf(queue.currentHead().offset()));
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
}

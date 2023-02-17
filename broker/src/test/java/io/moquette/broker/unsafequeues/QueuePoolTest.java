package io.moquette.broker.unsafequeues;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.BrokerConstants;
import io.moquette.broker.unsafequeues.Queue;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.moquette.broker.unsafequeues.Queue.LENGTH_HEADER_SIZE;
import static io.moquette.broker.unsafequeues.QueueTest.generatePayload;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueuePoolTest {

    private static final Logger LOG = LoggerFactory.getLogger(QueuePoolTest.class);

    private static final int PAGE_SIZE = BrokerConstants.DEFAULT_SEGMENTED_QUEUE_PAGE_SIZE;
    private static final int SEGMENT_SIZE = BrokerConstants.DEFAULT_SEGMENTED_QUEUE_SEGMENT_SIZE;

    @TempDir
    Path tempQueueFolder;

    @Test
    public void checkpointFileContainsCorrectReferences() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");
        queue.enqueue((ByteBuffer)ByteBuffer.wrap("AAAA".getBytes(StandardCharsets.UTF_8)));
        queue.force();
        queuePool.close();

        // verify
        final Path checkpointPath = tempQueueFolder.resolve("checkpoint.properties");
        final File checkpointFile = checkpointPath.toFile();
        assertTrue(checkpointFile.exists(), "Checkpoint file must be created");

        final Properties checkpoint = loadCheckpoint(checkpointPath);
        final int lastPage = Integer.parseInt(checkpoint.get("segments.last_page").toString());
        assertEquals(0, lastPage);
        final int lastSegment = Integer.parseInt(checkpoint.get("segments.last_segment").toString());
        assertEquals(1, lastSegment);

        assertEquals("test", checkpoint.get("queues.0.name"), "Queue name must match");
    }

    private Properties loadCheckpoint(Path checkpointPath) throws IOException {
        final FileReader fileReader;
        fileReader = new FileReader(checkpointPath.toFile());
        final Properties checkpointProps = new Properties();
        checkpointProps.load(fileReader);
        return checkpointProps;
    }

    @Test
    public void reloadQueuePoolAndCheckRestartFromWhereItLeft() throws QueueException, IOException {
        QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        Queue queue = queuePool.getOrCreate("test");
        queue.enqueue(ByteBuffer.wrap("AAAA".getBytes(StandardCharsets.UTF_8)));
        queue.force();
        queuePool.close();

        // reload
        queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        queue = queuePool.getOrCreate("test");
        queue.enqueue(ByteBuffer.wrap("BBBB".getBytes(StandardCharsets.UTF_8)));
        queue.force();
        queuePool.close();

        // verify
        final Path checkpointPath = tempQueueFolder.resolve("checkpoint.properties");
        final File checkpointFile = checkpointPath.toFile();
        assertTrue(checkpointFile.exists(), "Checkpoint file must be created");

        final Properties checkpoint = loadCheckpoint(checkpointPath);
        final int lastPage = Integer.parseInt(checkpoint.get("segments.last_page").toString());
        assertEquals(0, lastPage);
        final int lastSegment = Integer.parseInt(checkpoint.get("segments.last_segment").toString());
        assertEquals(1, lastSegment);

        assertEquals("test", checkpoint.get("queues.0.name"), "Queue name must match");
        assertEquals("15", checkpoint.get("queues.0.head_offset"), "Queue head must be 16 bytes over the start");
    }

    private TreeSet<QueuePool.SegmentRef> asTreeSet(QueuePool.SegmentRef... segments) {
        final TreeSet<QueuePool.SegmentRef> usedSegments = new TreeSet<>();
        usedSegments.addAll(Arrays.asList(segments));
        return usedSegments;
    }

    @Test
    public void checkRecreateHolesAtTheStartOfThePage() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final QueuePool.SegmentRef middleSegment = new QueuePool.SegmentRef(0, SEGMENT_SIZE);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(middleSegment));

        // Verify
        assertEquals(1, holes.size(), "Only the preceding segment should be created");
        QueuePool.SegmentRef singleHole = holes.get(0);
        assertEquals(0, singleHole.pageId);
        assertEquals(0, singleHole.offset);
    }

    @Test
    public void checkRecreateHolesAtTheStartOfThePageWith2OccupiedContiguousSegments() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final QueuePool.SegmentRef firstSegment = new QueuePool.SegmentRef(0, SEGMENT_SIZE);
        final QueuePool.SegmentRef secondSegment = new QueuePool.SegmentRef(0, 2 * SEGMENT_SIZE);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(firstSegment, secondSegment));

        // Verify
        assertEquals(1, holes.size(), "Only the preceding segment should be created");
        QueuePool.SegmentRef singleHole = holes.get(0);
        assertEquals(0, singleHole.pageId);
        assertEquals(0, singleHole.offset);
    }

    @Test
    public void checkRecreateHolesBeforeSecondPage() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final QueuePool.SegmentRef middleSegment = new QueuePool.SegmentRef(1, SEGMENT_SIZE);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(middleSegment));

        // Verify
        final int expectedHolesCount = (int) (PAGE_SIZE / SEGMENT_SIZE) + 1;
        assertEquals(expectedHolesCount, holes.size(), "The previous empty page is full of holes");
        for (int i = 0; i < expectedHolesCount - 1; i++) {
            final QueuePool.SegmentRef hole = holes.get(i);
            assertEquals(0, hole.pageId);
            assertEquals(i * SEGMENT_SIZE, hole.offset);
        }
        QueuePool.SegmentRef singleHole = holes.get(expectedHolesCount - 1);
        assertEquals(1, singleHole.pageId);
        assertEquals(0, singleHole.offset);
    }

    @Test
    public void checkRecreateHolesBetweenUsedSegmentsOnSamePage() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final QueuePool.SegmentRef initialSegment = new QueuePool.SegmentRef(0, 0);
        final QueuePool.SegmentRef middleSegment = new QueuePool.SegmentRef(0, 3 * SEGMENT_SIZE);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(initialSegment, middleSegment));

        // Verify
        assertEquals(2, holes.size());

        // first hole
        assertEquals(0, holes.get(0).pageId);
        assertEquals(SEGMENT_SIZE, holes.get(0).offset);
        // second hole
        assertEquals(0, holes.get(1).pageId);
        assertEquals(2 * SEGMENT_SIZE, holes.get(1).offset);
    }

    @Test
    public void checkRecreateHolesSpanningMultiplePages() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final QueuePool.SegmentRef initialSegment = new QueuePool.SegmentRef(0, 0);
        final QueuePool.SegmentRef middleSegment = new QueuePool.SegmentRef(2, 3 * SEGMENT_SIZE);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(initialSegment, middleSegment));

        // Verify
        final int holesInEmptyPage = (PAGE_SIZE / SEGMENT_SIZE);
        final int holesInFirstPage = holesInEmptyPage - 1;
        final int holesInLastPage = 3;
        final int expectedHolesCount = holesInFirstPage + holesInEmptyPage + holesInLastPage;
        assertEquals(expectedHolesCount, holes.size());

        // first page hole
        int i = 0;
        int expectedOffset = SEGMENT_SIZE;
        for (; i < holesInFirstPage; i++) {
            final QueuePool.SegmentRef hole = holes.get(i);
            assertEquals(0, hole.pageId);
            assertEquals(expectedOffset, hole.offset);
            expectedOffset += SEGMENT_SIZE;
        }

        // central empty pages
        expectedOffset = 0;
        for (; i < holesInFirstPage + holesInEmptyPage; i++) {
            final QueuePool.SegmentRef hole = holes.get(i);
            assertEquals(1, hole.pageId);
            assertEquals(expectedOffset, hole.offset);
            expectedOffset += SEGMENT_SIZE;
        }

        // tail page hole
        expectedOffset = 0;
        for (; i < expectedHolesCount; i++) {
            final QueuePool.SegmentRef hole = holes.get(i);
            assertEquals(2, hole.pageId);
            assertEquals(expectedOffset, hole.offset);
            expectedOffset += SEGMENT_SIZE;
        }
    }

    @Test
    public void checkRecreateHolesWhenSegmentAreAdjacentAndSpanningMultiplePages() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final QueuePool.SegmentRef initialSegment = new QueuePool.SegmentRef(0, PAGE_SIZE - SEGMENT_SIZE);
        final QueuePool.SegmentRef adjacentSegment = new QueuePool.SegmentRef(1, 0);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(initialSegment, adjacentSegment));

        // Verify
        assertEquals((PAGE_SIZE - SEGMENT_SIZE) / SEGMENT_SIZE, holes.size());
    }

    @Test
    @Disabled
    public void verifySingleReaderSingleWriterOnSingleQueuePool_with_955157_size_packet() throws QueueException, ExecutionException, InterruptedException, TimeoutException {
        templateSingleReaderSingleWriterOnSingleQueuePool(955157);
    }

    @Test
    @Disabled
    public void verifySingleReaderSingleWriterOnSingleQueuePool_with_random_size_packet() throws QueueException, ExecutionException, InterruptedException, TimeoutException, NoSuchAlgorithmException {
        final int payloadSize = SecureRandom.getInstanceStrong().nextInt(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE);
        templateSingleReaderSingleWriterOnSingleQueuePool(payloadSize);

    }

    private void templateSingleReaderSingleWriterOnSingleQueuePool(int payloadSize) throws QueueException, InterruptedException, ExecutionException, TimeoutException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        Queue queue = queuePool.getOrCreate("single_writer_single_reader");
        ExecutorService pool = Executors.newCachedThreadPool();
        int messagesToSend = 1000;

        LOG.info("Payload size: " + payloadSize);
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(payloadSize, (byte) 'a'));

        Future<?> senderFuture = pool.submit(createMessageSender(queue, payload, messagesToSend));
        Future<Integer> readerFuture = pool.submit(createMessageReader(queue, messagesToSend));

        senderFuture.get(60, TimeUnit.SECONDS);
        final int readBytes = readerFuture.get(60, TimeUnit.SECONDS);
        assertEquals(messagesToSend * payloadSize, readBytes);
    }

    private Runnable createMessageSender(Queue queue, ByteBuffer payload, int count) {
        return () -> {
            int payloadSize = payload.remaining();
            int sentBytes = 0;
            for (int i = 0; i < count; i++) {
                try {
                    ByteBuffer duplicate = payload.duplicate();
                    sentBytes += duplicate.remaining();
                    queue.enqueue(duplicate);
                } catch (QueueException e) {
                    throw new RuntimeException(e);
                }
            }
            LOG.debug("Finish to send data, " + count + " messages of size: " + payloadSize + " for a total of: " + sentBytes);
        };
    }

    private Callable<Integer> createMessageReader(Queue queue, int expectedMessages) {
        return () -> {
            try {
                int readBytes = 0;
                int receivedMessages = 0;
                do {
                    LOG.debug("receivedMessages {} ({} expected)", receivedMessages, expectedMessages);
                    Optional<ByteBuffer> readPayload = queue.dequeue();
                    if (readPayload.isPresent()) {
                        readBytes += readPayload.get().remaining();
                        receivedMessages++;
                    }
                } while (receivedMessages < expectedMessages);
                LOG.debug("Received messages: " + receivedMessages);
                return readBytes;
            } catch (QueueException e) {
                throw new RuntimeException(e);
            }
        };
    }

    int result = 0;

    @Test
    @Disabled
    public void testMultipleWritersSingleReader() throws QueueException, NoSuchAlgorithmException, ExecutionException, InterruptedException, TimeoutException {
        final int payloadSize = 152433/*SecureRandom.getInstanceStrong().nextInt(Segment.SIZE / 2 - LENGTH_HEADER_SIZE)*/;
        LOG.info("Payload size: " + payloadSize);
        final QueuePool queuePool = QueuePool.loadQueues(/*tempQueueFolder*/Paths.get("/tmp/test_dir/"), PAGE_SIZE, SEGMENT_SIZE);
        Queue queue = queuePool.getOrCreate("multiple_writers_single_reader");
        ExecutorService pool = Executors.newCachedThreadPool();
        int messagesToSend = 1000;

        ByteBuffer payloadA = ByteBuffer.wrap(generatePayload(payloadSize, (byte) 'a'));
        ByteBuffer payloadB = ByteBuffer.wrap(generatePayload(payloadSize, (byte) 'b'));

        final Thread threadWriterA = new Thread(createMessageSender(queue, payloadA, messagesToSend / 2));
        threadWriterA.setName("Writer-A");

        final Thread threadWriterB = new Thread(createMessageSender(queue, payloadB, messagesToSend / 2));
        threadWriterB.setName("Writer-B");

        final Thread threadReader = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    result = createMessageReader(queue, messagesToSend).call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        threadReader.setName("Reader");

        threadWriterA.start();
        threadWriterB.start();
        threadReader.start();
        threadWriterA.join(60_000);
        threadWriterB.join(60_000);
        threadReader.join(60_000);


//        Future<?> senderFutureA = pool.submit(createMessageSender(queue, payloadA, messagesToSend / 2));
//        Future<?> senderFutureB = pool.submit(createMessageSender(queue, payloadB, messagesToSend / 2));
//        Future<Integer> readerFuture = pool.submit(createMessageReader(queue, messagesToSend));

//        senderFutureA.get(60, TimeUnit.SECONDS);
//        senderFutureB.get(60, TimeUnit.SECONDS);
//        final int readBytes = readerFuture.get(60, TimeUnit.SECONDS);
        assertEquals(messagesToSend * payloadSize, result);
    }
}

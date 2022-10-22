package io.moquette.broker.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.moquette.broker.queue.Queue.LENGTH_HEADER_SIZE;
import static io.moquette.broker.queue.QueueTest.generatePayload;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueuePoolTest {

    private static final Logger LOG = LoggerFactory.getLogger(QueuePoolTest.class);

    @TempDir
    Path tempQueueFolder;

    @Test
    public void checkpointFileContainsCorrectReferences() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
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
        QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        Queue queue = queuePool.getOrCreate("test");
        queue.enqueue(ByteBuffer.wrap("AAAA".getBytes(StandardCharsets.UTF_8)));
        queue.force();
        queuePool.close();

        // reload
        queuePool = QueuePool.loadQueues(tempQueueFolder);
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
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final QueuePool.SegmentRef middleSegment = new QueuePool.SegmentRef(0, Segment.SIZE);

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
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final QueuePool.SegmentRef firstSegment = new QueuePool.SegmentRef(0, Segment.SIZE);
        final QueuePool.SegmentRef secondSegment = new QueuePool.SegmentRef(0, 2 * Segment.SIZE);

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
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final QueuePool.SegmentRef middleSegment = new QueuePool.SegmentRef(1, Segment.SIZE);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(middleSegment));

        // Verify
        final int expectedHolesCount = (int) (PagedFilesAllocator.PAGE_SIZE / Segment.SIZE) + 1;
        assertEquals(expectedHolesCount, holes.size(), "The previous empty page is full of holes");
        for (int i = 0; i < expectedHolesCount - 1; i++) {
            final QueuePool.SegmentRef hole = holes.get(i);
            assertEquals(0, hole.pageId);
            assertEquals(i * Segment.SIZE, hole.offset);
        }
        QueuePool.SegmentRef singleHole = holes.get(expectedHolesCount - 1);
        assertEquals(1, singleHole.pageId);
        assertEquals(0, singleHole.offset);
    }

    @Test
    public void checkRecreateHolesBetweenUsedSegmentsOnSamePage() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final QueuePool.SegmentRef initialSegment = new QueuePool.SegmentRef(0, 0);
        final QueuePool.SegmentRef middleSegment = new QueuePool.SegmentRef(0, 3 * Segment.SIZE);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(initialSegment, middleSegment));

        // Verify
        assertEquals(2, holes.size());

        // first hole
        assertEquals(0, holes.get(0).pageId);
        assertEquals(Segment.SIZE, holes.get(0).offset);
        // second hole
        assertEquals(0, holes.get(1).pageId);
        assertEquals(2 * Segment.SIZE, holes.get(1).offset);
    }

    @Test
    public void checkRecreateHolesSpanningMultiplePages() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final QueuePool.SegmentRef initialSegment = new QueuePool.SegmentRef(0, 0);
        final QueuePool.SegmentRef middleSegment = new QueuePool.SegmentRef(2, 3 * Segment.SIZE);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(initialSegment, middleSegment));

        // Verify
        final int holesInEmptyPage = (PagedFilesAllocator.PAGE_SIZE / Segment.SIZE);
        final int holesInFirstPage = holesInEmptyPage - 1;
        final int holesInLastPage = 3;
        final int expectedHolesCount = holesInFirstPage + holesInEmptyPage + holesInLastPage;
        assertEquals(expectedHolesCount, holes.size());

        // first page hole
        int i = 0;
        int expectedOffset = Segment.SIZE;
        for (; i < holesInFirstPage; i++) {
            final QueuePool.SegmentRef hole = holes.get(i);
            assertEquals(0, hole.pageId);
            assertEquals(expectedOffset, hole.offset);
            expectedOffset += Segment.SIZE;
        }

        // central empty pages
        expectedOffset = 0;
        for (; i < holesInFirstPage + holesInEmptyPage; i++) {
            final QueuePool.SegmentRef hole = holes.get(i);
            assertEquals(1, hole.pageId);
            assertEquals(expectedOffset, hole.offset);
            expectedOffset += Segment.SIZE;
        }

        // tail page hole
        expectedOffset = 0;
        for (; i < expectedHolesCount; i++) {
            final QueuePool.SegmentRef hole = holes.get(i);
            assertEquals(2, hole.pageId);
            assertEquals(expectedOffset, hole.offset);
            expectedOffset += Segment.SIZE;
        }
    }

    @Test
    public void checkRecreateHolesWhenSegmentAreAdjacentAndSpanningMultiplePages() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final QueuePool.SegmentRef initialSegment = new QueuePool.SegmentRef(0, PagedFilesAllocator.PAGE_SIZE - Segment.SIZE);
        final QueuePool.SegmentRef adjacentSegment = new QueuePool.SegmentRef(1, 0);

        // Exercise
        final List<QueuePool.SegmentRef> holes = queuePool.recreateSegmentHoles(asTreeSet(initialSegment, adjacentSegment));

        // Verify
        assertEquals((PagedFilesAllocator.PAGE_SIZE - Segment.SIZE) / Segment.SIZE, holes.size());
    }

    @Test
    public void verifySingleReaderSingleWriterOnSingleQueuePool_with_955157_size_packet() throws QueueException, ExecutionException, InterruptedException, TimeoutException {
        templateSingleReaderSingleWriterOnSingleQueuePool(955157);
    }

    @Test
    public void verifySingleReaderSingleWriterOnSingleQueuePool_with_random_size_packet() throws QueueException, ExecutionException, InterruptedException, TimeoutException, NoSuchAlgorithmException {
        final int payloadSize = SecureRandom.getInstanceStrong().nextInt(Segment.SIZE / 2 - LENGTH_HEADER_SIZE);
        templateSingleReaderSingleWriterOnSingleQueuePool(payloadSize);

    }

    private void templateSingleReaderSingleWriterOnSingleQueuePool(int payloadSize) throws QueueException, InterruptedException, ExecutionException, TimeoutException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
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
                ByteBuffer readPayload;
                int receivedMessages = 0;
                do {
                    readPayload = queue.dequeue();
                    if (readPayload != null) {
                        readBytes += readPayload.remaining();
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
}

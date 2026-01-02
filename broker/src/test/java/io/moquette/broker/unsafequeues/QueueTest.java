package io.moquette.broker.unsafequeues;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.moquette.BrokerConstants;
import io.moquette.broker.unsafequeues.Queue;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Consumer;

import static io.moquette.broker.unsafequeues.Queue.LENGTH_HEADER_SIZE;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class QueueTest {

    private static final int PAGE_SIZE = BrokerConstants.DEFAULT_SEGMENTED_QUEUE_PAGE_SIZE;
    private static final int SEGMENT_SIZE = BrokerConstants.DEFAULT_SEGMENTED_QUEUE_SEGMENT_SIZE;

    @TempDir
    Path tempQueueFolder;

    private void assertContainsOnly(char expectedChar, byte[] verify) {
        for (int i = 0; i < verify.length; i++) {
            if (verify[i] != expectedChar) {
                fail(String.format("Expected %c but found %c in %c%c%c", expectedChar, verify[i], verify[i-1], verify[i], verify[i+1]));
            }
        }
    }

    private void assertContainsOnly(char expectedChar, ByteBuffer verify) {
        int pos = verify.position();
        while (verify.hasRemaining()) {
            final byte readChar = verify.get();
            pos++;
            if (readChar != expectedChar) {
                fail(String.format("Expected %c but found %c at position %d", expectedChar, readChar, pos));
            }
        }
    }

    private void assertContainsOnly(char expectedChar, ByteBuffer verify, int expectedSize) {
        int pos = verify.position();
        int countChars = 0;
        while (verify.hasRemaining()) {
            final byte readChar = verify.get();
            pos++;
            countChars++;
            if (readChar != expectedChar) {
                fail(String.format("Expected %c but found %c at position %d", expectedChar, readChar, pos));
            }
        }
        assertEquals(expectedSize, countChars);
    }


    static byte[] generatePayload(int numBytes) {
        return generatePayload(numBytes, (byte) 'A');
    }

    static byte[] generatePayload(int numBytes, byte c) {
        final byte[] payload = new byte[numBytes];
        for (int i = 0; i < numBytes; i++) {
            payload[i] = c;
        }
        return payload;
    }

    @Test
    public void testSizesFitTogether() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {new PagedFilesAllocator(null, 1000, 499, 0, 0);});
    }

    @Test
    public void basicNoBlockEnqueue() throws QueueException, IOException {
        final MappedByteBuffer pageBuffer = Utils.createPageFile();

        final Segment head = new Segment(pageBuffer, new SegmentPointer(0, 0), new SegmentPointer(0, 1024));
        final VirtualPointer currentHead = VirtualPointer.buildUntouched();
        final Queue queue = new Queue("test", head, currentHead, head, currentHead, new DummySegmentAllocator(), (name, segment) -> {
            // NOOP
        }, null);

        // generate byte array to insert.
        ByteBuffer payload = randomPayload(128);

        queue.enqueue(payload);
    }

    private ByteBuffer randomPayload(int dataSize) {
        byte[] payload = new byte[dataSize];
        new Random().nextBytes(payload);

        return (ByteBuffer) ByteBuffer.wrap(payload);
    }

    @Test
    public void newlyQueueIsEmpty() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        assertTrue(queue.isEmpty(), "Freshly created queue must be empty");
    }

    @Test
    public void consumedQueueIsEmpty() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");
        queue.enqueue(ByteBuffer.wrap("AAAA".getBytes(StandardCharsets.UTF_8)));
        Optional<ByteBuffer> data = queue.dequeue();
        assertTrue(data.isPresent(), "Some payload is retrieved");

        assertEquals(4, data.get().remaining(), "Payload contains what's expected");

        assertTrue(queue.isEmpty(), "Queue must be empty after consuming it");
    }

    @Test
    public void insertSomeDataIntoNewQueue() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");
        queue.enqueue(ByteBuffer.wrap("AAAA".getBytes(StandardCharsets.UTF_8)));

        // verify
        final HashSet<String> fileset = new HashSet<>(Arrays.asList(tempQueueFolder.toFile().list()));
        assertEquals(2, fileset.size());
        assertTrue(fileset.contains("checkpoint.properties"), "Checkpoint file must be created");
        assertTrue(fileset.contains("0.page"), "One page file must be created");

        final Path pageFile = tempQueueFolder.resolve("0.page");
        verifyFile(pageFile, 9, rawContent -> {
            assertEquals(4, rawContent.getInt(), "First 4 bytes contains the length");
            assertEquals('A', rawContent.get());
            assertEquals('A', rawContent.get());
            assertEquals('A', rawContent.get());
            assertEquals('A', rawContent.get());
            assertEquals(0, rawContent.get());
        });
    }

    private void verifyFile(Path file, int bytesToRead, Consumer<ByteBuffer> verifier) throws IOException {
        final FileChannel fc = FileChannel.open(file, StandardOpenOption.READ);
        final ByteBuffer rawContent = ByteBuffer.allocate(bytesToRead);
        final int read = fc.read(rawContent);
        assertEquals(bytesToRead, read);
        rawContent.flip();

        verifier.accept(rawContent);
    }

    @Test
    public void insertDataTriggerCreationOfNewPageFile() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        // one page is 64 MB so the loop count to fill it is 64 * 1024
        // 4 bytes are left for length so that each time are inserted 1024 bytes, 4 header and 1020 payload
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE));
        for (int i = 0; i < 64; i++) {
            writeMessages(queue, payload, 1024);
        }

        // check the 2 files are created
        HashSet<String> fileset = new HashSet<>(Arrays.asList(tempQueueFolder.toFile().list()));
        assertEquals(2, fileset.size());
        assertTrue(fileset.contains("checkpoint.properties"), "Checkpoint file must be created");
        assertTrue(fileset.contains("0.page"),
            "One page file must be created");

        // Exercise
        // some data to force create a new page
        final ByteBuffer crossingPayload = ByteBuffer.wrap(generatePayload(10, (byte) 'B'));
        queue.enqueue(crossingPayload);

        // Verify
        fileset = new HashSet<>(Arrays.asList(tempQueueFolder.toFile().list()));
        assertEquals(3, fileset.size());
        assertTrue(fileset.contains("checkpoint.properties"), "Checkpoint file must be created");
        assertTrue(fileset.contains("0.page"), "First page file must be created");
        assertTrue(fileset.contains("1.page"), "Second page file must be created");
    }

    @Test
    public void insertWithAnHeaderThatCrossSegments() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        // fill the segment, inserting last message crossing the boundary
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE));
        writeMessages(queue, payload, (4 * 1024) - 1);
        // at the end we have 1024 bytes free, so fill only 1022 bytes of that
        payload = ByteBuffer.wrap(generatePayload(1022 - LENGTH_HEADER_SIZE));
        payload.rewind();
        queue.enqueue(payload);

        // Exercise
        ByteBuffer crossingPayload = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE, (byte) 'B'));
        queue.enqueue(crossingPayload);

        // Verify
        final MappedByteBuffer page = Utils.openPageFile(tempQueueFolder.resolve("0.page"), PAGE_SIZE);
        final int beforeLastMessagePayload = 4 * 1024 * 1024 + 2;
        final ByteBuffer crossingSegment = (ByteBuffer) page.position(beforeLastMessagePayload);
//        final int msgLength = crossingSegment.getInt();

//        assertEquals(1028 - LENGTH_HEADER_SIZE, msgLength);
//        byte[] probe = new byte[msgLength];
        byte[] probe = new byte[1020];
        crossingSegment.get(probe);
        assertContainsOnly('B', probe);
    }

    @Test
    public void insertDataCrossingSegmentBoundary() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        // one segment is 4MB 4 * 1024 * payload
        // so send (4 * 1024) - 1 payloads of 1024 and then send
        // a payload of 1028 (4 bytes over remaining space)

        // 4 bytes are left for length so that each time are inserted 1024 bytes, 4 header and 1020 payload
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE));
        writeMessages(queue, payload, (4 * 1024) - 1);

        // Experiment
        ByteBuffer crossingPayload = ByteBuffer.wrap(generatePayload(1028 - LENGTH_HEADER_SIZE, (byte) 'B'));
        queue.enqueue(crossingPayload);
        queue.force();
        queuePool.close();

        // Verify
        final MappedByteBuffer page = Utils.openPageFile(tempQueueFolder.resolve("0.page"), PAGE_SIZE);
        final int beforeLastMessage = ((4 * 1024) - 1) * 1024;
        final ByteBuffer crossingSegment = (ByteBuffer) page.position(beforeLastMessage);
        final int msgLength = crossingSegment.getInt();

        assertEquals(1028 - LENGTH_HEADER_SIZE, msgLength);
        byte[] probe = new byte[msgLength];
        crossingSegment.get(probe);
        assertContainsOnly('B', probe);
    }

    @Test
    public void insertDataBiggerThanASegment() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        // one segment is 4MB 4 * 1024 * payload
        // so send (4 * 1024) - 1 payloads of 1024 and then send
        // a payload of 1028 (4 bytes over remaining space)
        // 4 bytes are left for length so that each time are inserted 1024 bytes, 4 header and 1020 payload
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE));
        writeMessages(queue, payload, (4 * 1024) - 1);

        // Experiment
        // 1024 + 4 * 1024 * 1024 + 16 bytes
        int moreThanOneSegment = 1024 + 4 * 1024 * 1024 + 16;
        ByteBuffer crossingMultipleSegmentPayload = ByteBuffer.wrap(generatePayload(moreThanOneSegment, (byte) 'B'));
        queue.enqueue(crossingMultipleSegmentPayload);
        queue.force();
        queuePool.close();

        // Verify
        final MappedByteBuffer page = Utils.openPageFile(tempQueueFolder.resolve("0.page"), PAGE_SIZE);
        final int beforeLastMessage = ((4 * 1024) - 1) * 1024;
        final ByteBuffer crossingSegment = (ByteBuffer) page.position(beforeLastMessage);
        final int msgLength = crossingSegment.getInt();

        assertEquals(moreThanOneSegment, msgLength);
        byte[] probe = new byte[msgLength];
        crossingSegment.get(probe);
        assertContainsOnly('B', probe);
    }

    @Test
    public void readFromEmptyQueue() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        assertFalse(queue.dequeue().isPresent(), "Pulling from empty queue MUST return null value");
    }

    @Test
    public void readInSameSegment() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        final ByteBuffer message = ByteBuffer.wrap("Hello World!".getBytes(StandardCharsets.UTF_8));
        queue.enqueue(message);

        //Exercise
        final ByteBuffer result = queue.dequeue().get();
        final String readMessage = Utils.bufferToString(result);
        assertEquals("Hello World!", readMessage, "Read the same message tha was enqueued");
    }

    @Test
    public void readCrossingSegment() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        // fill the segment, inserting last message crossing the boundary
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE));
        for (int i = 0; i < (4 * 1024) - 1; i++) {
            payload.rewind();
            queue.enqueue(payload);
            queue.dequeue();
        }

        ByteBuffer crossingPayload = ByteBuffer.wrap(generatePayload(1028 - LENGTH_HEADER_SIZE, (byte) 'B'));
        queue.enqueue(crossingPayload);

        //Exercise
        final ByteBuffer message = queue.dequeue().get();
        assertEquals(1028 - LENGTH_HEADER_SIZE, message.remaining(), "There must be 1024 'B' letters");
        assertContainsOnly('B', message);
    }

    @Test
    public void readWithHeaderCrossingSegments() throws QueueException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        // fill the segment, inserting last message crossing the boundary
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE));
        for (int i = 0; i < (4 * 1024) - 1; i++) {
            payload.rewind();
            queue.enqueue(payload);
            queue.dequeue();
        }
        // at the end we have 1024 bytes free, so fill only 1022 bytes of that
        payload = ByteBuffer.wrap(generatePayload(1022 - LENGTH_HEADER_SIZE));
        payload.rewind();
        queue.enqueue(payload);
        queue.dequeue();

        // write a payload's header with 2 bytes in previous and 2 in next segment
        ByteBuffer crossingPayload = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE, (byte) 'B'));
        queue.enqueue(crossingPayload);

        //Exercise
        final ByteBuffer message = queue.dequeue().get();
        assertEquals(1024 - LENGTH_HEADER_SIZE, message.remaining(), "There must be 1020 'B' letters");
        assertContainsOnly('B', message);
    }

    @Test
    public void readCrossingPages() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test");

        // fill all segments less one in a page
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE));
        int messageSize = payload.remaining() + LENGTH_HEADER_SIZE;
        final int loopToFill = PAGE_SIZE / messageSize;
        writeMessages(queue, payload, loopToFill - 1);

        assertEquals(1, countPages(tempQueueFolder));
        assertEquals(PAGE_SIZE - messageSize, queue.currentHead().logicalOffset() + 1,
            "head must be one message size (1024) from the end of the segment");

        // Exercise
        payload = ByteBuffer.wrap(generatePayload(2048 - LENGTH_HEADER_SIZE, (byte) 'B'));
        queue.enqueue(payload);

        // Verify
        assertEquals(2, countPages(tempQueueFolder));
    }

    private long countPages(Path tempQueueFolder) throws IOException {
        return Files.list(tempQueueFolder).filter(p -> p.toString().endsWith(".page")).count();
    }

    @Test
    public void interleavedQueueSegments() throws QueueException, IOException {
        // first segment queue A, second segment queue B and so on, in stripped fashion.
        // writes and read pass single page borders, checking everything is fine
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queueA = queuePool.getOrCreate("testA");
        final Queue queueB = queuePool.getOrCreate("testB");

        ByteBuffer payloadQueueA = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE, (byte) 'A'));
        ByteBuffer payloadQueueB = ByteBuffer.wrap(generatePayload(1024 - LENGTH_HEADER_SIZE, (byte) 'B'));
        int messageSize = payloadQueueA.remaining() + LENGTH_HEADER_SIZE;

        // Exercise
        final int numPages = 2;
        final int segmentsToFill = numPages * PAGE_SIZE / SEGMENT_SIZE;
        final int messagesInSegment = SEGMENT_SIZE / messageSize;
        for (int i = 0; i < segmentsToFill; i++) {
            if (isEven(i)) {
                writeMessages(queueA, payloadQueueA, messagesInSegment);
            } else {
                writeMessages(queueB, payloadQueueB, messagesInSegment);
            }
        }

        // Verify
        assertEquals(numPages, countPages(tempQueueFolder));
        final int numMessagesInQueue = PAGE_SIZE / messageSize;
        verifyReadingFromQueue(numMessagesInQueue, queueA, 'A', 1024 - LENGTH_HEADER_SIZE);
        verifyReadingFromQueue(numMessagesInQueue, queueB, 'B', 1024 - LENGTH_HEADER_SIZE);
    }

    private void verifyReadingFromQueue(int numMessagesInQueue, Queue queue, char ch, int expectedPayloadSize) throws QueueException {
        for (int i = 0; i < numMessagesInQueue; i++) {
            final ByteBuffer payload = queue.dequeue().get();
            assertContainsOnly(ch, payload, expectedPayloadSize);
        }
    }

    private void writeMessages(Queue targetQueue, ByteBuffer payload, int messagesToWrite) throws QueueException {
        for (int i = 0; i < messagesToWrite; i++) {
            payload.rewind();
            targetQueue.enqueue(payload);
        }
    }

    private boolean isEven(int i) {
        return i % 2 == 0;
    }

    @Test
    public void physicalBackwardSegment() throws IOException, QueueException {
        // Artificially create a queue composed of segment(2) and segment(1), inverted in order, verify
        // if read and write properly.pageBuffer.
        final Path pageFile = this.tempQueueFolder.resolve("0.page");
        final OpenOption[] openOptions = {StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        FileChannel fileChannel = FileChannel.open(pageFile, openOptions);
        final MappedByteBuffer pageBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
        // segment with only one message of B letters
        pageBuffer.putInt(SEGMENT_SIZE - LENGTH_HEADER_SIZE);
        pageBuffer.put(generatePayload(SEGMENT_SIZE - LENGTH_HEADER_SIZE, (byte) 'B'));
        // segment with only one message of A letters
        pageBuffer.putInt(SEGMENT_SIZE - LENGTH_HEADER_SIZE);
        pageBuffer.put(generatePayload(SEGMENT_SIZE - LENGTH_HEADER_SIZE, (byte) 'A'));
        pageBuffer.force();
        fileChannel.close();

        // create the checkpoint file with the inverted segments
        Properties checkpoint = new Properties();
//        checkpoint.properties
        checkpoint.put("queues.0.name", "test_inverted");
        checkpoint.put("queues.0.segments", "(0, 0), (0, " + SEGMENT_SIZE + ")");
        checkpoint.put("queues.0.head_offset", Integer.toString(SEGMENT_SIZE - 1));
        checkpoint.put("queues.0.tail_offset", Integer.toString(-1));
        checkpoint.put("segments.last_page", Integer.toString(0));
        checkpoint.put("segments.last_segment", Integer.toString(2));
        File propsFile = this.tempQueueFolder.resolve("checkpoint.properties").toFile();
        final FileWriter propsWriter = new FileWriter(propsFile);
        checkpoint.store(propsWriter, "test checkpoint file to verify loading of inverted segments");

        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test_inverted");

        assertContainsOnly('A', queue.dequeue().get(), SEGMENT_SIZE - LENGTH_HEADER_SIZE);
        assertContainsOnly('B', queue.dequeue().get(), SEGMENT_SIZE - LENGTH_HEADER_SIZE);
    }

    @Test
    public void reopenQueueWithSomeDataInto() throws QueueException {
        // given a queue wth some data split across multiple segments
        final QueuePool queuePoolA = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queueA = queuePoolA.getOrCreate("testA");
        queueA.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'a')));
        queueA.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'A')));
        queueA.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'b')));
        queueA.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'B')));
        // when it's closed and reopened
        queueA.force();
        queuePoolA.close();

        // then the consumption must happen in the same order
        final Queue reopened = queuePoolA.getOrCreate("testA");
        assertContainsOnly('a', reopened.dequeue().get(), SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE);
        assertContainsOnly('A', reopened.dequeue().get(), SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE);
        assertContainsOnly('b', reopened.dequeue().get(), SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE);
        assertContainsOnly('B', reopened.dequeue().get(), SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE);
    }

    @Test
    public void writeTestSuiteToVerifyPagedFilesAllocatorDoesntCreateExternalFragmentation() throws QueueException, IOException {
        // write 2 segments, consume one segment, next segment allocated should be one just freed.0
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test_external_fragmentation");

        // fill first segment (0, 0)
        queue.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'a')));
        queue.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'A')));

        // fill second segment (0, 4194304)
        queue.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'b')));
        queue.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'B')));

        // consume first segment
        assertContainsOnly('a', queue.dequeue().get(), SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE);
        assertContainsOnly('A', queue.dequeue().get(), SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE);

        // Exercise
        // write new data, should go in first freed segment
        queue.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'c')));
        queuePool.close();

        // Verify
        // checkpoint contains che correct order, (0,0), (0, 4194304)
        final Properties checkpointProps = loadCheckpointFile(tempQueueFolder);

        final String segmentRefs = checkpointProps.getProperty("queues.0.segments");
        assertEquals("(0, 0), (0, 4194304)", segmentRefs);
    }

    @Test
    public void reopenQueueWithFragmentation() throws QueueException, IOException {
        // write 2 segments, consume one segment, next segment allocated should be one just freed.0
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue queue = queuePool.getOrCreate("test_external_fragmentation");

        // fill first segment (0, 0)
        queue.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'a')));
        queue.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'A')));

        // fill second segment (0, 4194304)
        queue.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'b')));
        queue.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'B')));

        // consume first segment
        assertContainsOnly('a', queue.dequeue().get(), SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE);
        assertContainsOnly('A', queue.dequeue().get(), SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE);

        queue.force();
        queuePool.close();

        // Exercise
        // reopen the queue
        final QueuePool recreatedQueuePool = QueuePool.loadQueues(tempQueueFolder, PAGE_SIZE, SEGMENT_SIZE);
        final Queue reopened = recreatedQueuePool.getOrCreate("test_external_fragmentation");
        // write new data, should go in first freed segment
        reopened.enqueue(ByteBuffer.wrap(generatePayload(SEGMENT_SIZE / 2 - LENGTH_HEADER_SIZE, (byte) 'c')));
        recreatedQueuePool.close();

        // Verify
        // checkpoint contains che correct order, (0,0), (0, 4194304)
        final Properties checkpointProps = loadCheckpointFile(tempQueueFolder);

        final String segmentRefs = checkpointProps.getProperty("queues.0.segments");
        assertEquals("(0, 0), (0, 4194304)", segmentRefs);
    }

    @Test
    public void testPageFileReuse() throws QueueException, IOException {
        // Use smaller segmants and pages for quicker testing.
        final int kb = 1024;
        // Pages with only 4 segments, for quicker testing
        final int queuePageSize = 16 * kb;
        final int queueSegmentSize = 4 * kb;
        // write 2 segments, consume one segment, next segment allocated should be one just freed.0
        QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder, queuePageSize, queueSegmentSize);
        Queue queue1 = queuePool.getOrCreate("test_external_fragmentation");

        byte[] bytes = new byte[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n'};

        // fill seven segments (almost the full 8 we have)
        // This should force two files to open.
        for (int i = 0; i < 7; i++) {
            queue1.enqueue(ByteBuffer.wrap(generatePayload(queueSegmentSize / 2 - LENGTH_HEADER_SIZE, bytes[i * 2])));
            queue1.enqueue(ByteBuffer.wrap(generatePayload(queueSegmentSize / 2 - LENGTH_HEADER_SIZE, bytes[i * 2 + 1])));
        }

        // Create a new queue
        final Queue queue2 = queuePool.getOrCreate("test_external_fragmentation2");

        // Write one segment (filling the second page)
        queue2.enqueue(ByteBuffer.wrap(generatePayload(queueSegmentSize / 2 - LENGTH_HEADER_SIZE, bytes[0])));
        queue2.enqueue(ByteBuffer.wrap(generatePayload(queueSegmentSize / 2 - LENGTH_HEADER_SIZE, bytes[1])));

        // Release the first queue
        queue1.force();
        queue1.close();

        // Fill another six segments.
        // This should not open more files.
        for (int i = 1; i < 7; i++) {
            queue2.enqueue(ByteBuffer.wrap(generatePayload(queueSegmentSize / 2 - LENGTH_HEADER_SIZE, bytes[i * 2])));
            queue2.enqueue(ByteBuffer.wrap(generatePayload(queueSegmentSize / 2 - LENGTH_HEADER_SIZE, bytes[i * 2 + 1])));
        }

        queue2.force();
        queuePool.close();

        // Verify
        // checkpoint contains che correct order, (0,0), (0, 4194304)
        Properties checkpointProps3 = loadCheckpointFile(tempQueueFolder);

        // We should now have segments 6, 5, 4, 3, 2, 1, 8
        // or, 2.2, 2.1, 1.4, 1.3, 1.2, 1.1, 2.4
        final String segmentRefs = checkpointProps3.getProperty("queues.0.segments");
        assertEquals("(1, 4096), (1, 0), (0, 12288), (0, 8192), (0, 4096), (0, 0), (1, 12288)", segmentRefs);
    }

    private Properties loadCheckpointFile(Path dir) throws IOException {
        final Path checkpointPath = dir.resolve("checkpoint.properties");
        final FileReader fileReader = new FileReader(checkpointPath.toFile());
        final Properties checkpointProps = new Properties();
        checkpointProps.load(fileReader);
        return checkpointProps;
    }
}

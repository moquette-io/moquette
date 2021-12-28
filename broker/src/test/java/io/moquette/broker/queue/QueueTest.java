package io.moquette.broker.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class QueueTest {

    @TempDir
    Path tempQueueFolder;

    private void assertContainsOnly(char expectedChar, byte[] verify) {
        for (int i = 0; i < verify.length; i++) {
            if (verify[i] != expectedChar) {
                fail(String.format("Expected %c but found %c in %c%c%c", expectedChar, verify[i], verify[i-1], verify[i], verify[i+1]));
            }
        }
    }

    private byte[] generatePayload(int numBytes) {
        return generatePayload(numBytes, (byte) 'A');
    }

    private byte[] generatePayload(int numBytes, byte c) {
        final byte[] payload = new byte[numBytes];
        for (int i = 0; i < numBytes; i++) {
            payload[i] = c;
        }
        return payload;
    }

    @Test
    public void basicNoBlockEnqueue() throws QueueException, IOException {
        final MappedByteBuffer pageBuffer = Utils.createPageFile();

        final Segment head = new Segment(pageBuffer, new SegmentPointer(0,0), new SegmentPointer(0, 1024));
        final SegmentPointer currentHead = new SegmentPointer(0, 0);
        final Queue queue = new Queue("test", head, currentHead, new DummySegmentAllocator(), (name, segment) -> {
            // NOOP
        });

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
    public void insertSomeDataIntoNewQueue() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
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
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final Queue queue = queuePool.getOrCreate("test");

        // one page is 64 MB so the loop count to fill it is 64 * 1024

       // 4 bytes are left for length so that each time are inserted 1024 bytes, 4 header and 1020 payload
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - 4));
        for (int i = 0; i < 64; i++) {
            for (int j = 0; j < 1024; j++) {
                payload.rewind();
                queue.enqueue(payload);
            }
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
    public void insertDataCrossingSegmentBoundary() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final Queue queue = queuePool.getOrCreate("test");

        // one segment is 4MB 4 * 1024 * payload
        // so send (4 * 1024) - 1 payloads of 1024 and then send
        // a payload of 1028 (4 bytes over remaining space)

        // 4 bytes are left for length so that each time are inserted 1024 bytes, 4 header and 1020 payload
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - 4));
        for (int i = 0; i < (4 * 1024) - 1; i++) {
            payload.rewind();
            queue.enqueue(payload);
        }

        // Experiment
        ByteBuffer crossingPayload = ByteBuffer.wrap(generatePayload(1028 - 4, (byte) 'B'));
        queue.enqueue(crossingPayload);
        queue.force();
        queuePool.close();

        // Verify
        final MappedByteBuffer page = Utils.openPageFile(tempQueueFolder.resolve("0.page"));
        final int beforeLastMessage = ((4 * 1024) - 1) * 1024;
        final ByteBuffer crossingSegment = (ByteBuffer) page.position(beforeLastMessage);
        final int msgLength = crossingSegment.getInt();

        assertEquals(1028 - 4, msgLength);
        byte[] probe = new byte[msgLength];
        crossingSegment.get(probe);
        assertContainsOnly('B', probe);
    }

    @Test
    public void insertDataBiggerThanASegment() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final Queue queue = queuePool.getOrCreate("test");

        // one segment is 4MB 4 * 1024 * payload
        // so send (4 * 1024) - 1 payloads of 1024 and then send
        // a payload of 1028 (4 bytes over remaining space)

        // 4 bytes are left for length so that each time are inserted 1024 bytes, 4 header and 1020 payload
        ByteBuffer payload = ByteBuffer.wrap(generatePayload(1024 - 4));
        for (int i = 0; i < (4 * 1024) - 1; i++) {
            payload.rewind();
            queue.enqueue(payload);
        }

        // Experiment
        // 1024 + 4 * 1024 * 1024 + 16 bytes
        int moreThanOneSegment = 1024 + 4*1024*1024 + 16;
        ByteBuffer crossingMultipleSegmentPayload = ByteBuffer.wrap(generatePayload(moreThanOneSegment, (byte) 'B'));
        queue.enqueue(crossingMultipleSegmentPayload);
        queue.force();
        queuePool.close();

        // Verify
        final MappedByteBuffer page = Utils.openPageFile(tempQueueFolder.resolve("0.page"));
        final int beforeLastMessage = ((4 * 1024) - 1) * 1024;
        final ByteBuffer crossingSegment = (ByteBuffer) page.position(beforeLastMessage);
        final int msgLength = crossingSegment.getInt();

        assertEquals(moreThanOneSegment, msgLength);
        byte[] probe = new byte[msgLength];
        crossingSegment.get(probe);
        assertContainsOnly('B', probe);
    }
}

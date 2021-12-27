package io.moquette.broker.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class QueuePoolTest {

    @TempDir
    Path tempQueueFolder;

    @Test
    public void checkpointFileContainsCorrectReferences() throws QueueException, IOException {
        final QueuePool queuePool = QueuePool.loadQueues(tempQueueFolder);
        final Queue queue = queuePool.getOrCreate("test");
        queue.enqueue("AAAA".getBytes(StandardCharsets.UTF_8));
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
        queue.enqueue("AAAA".getBytes(StandardCharsets.UTF_8));
        queue.force();
        queuePool.close();

        // reload
        queuePool = QueuePool.loadQueues(tempQueueFolder);
        queue = queuePool.getOrCreate("test");
        queue.enqueue("BBBB".getBytes(StandardCharsets.UTF_8));
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
}

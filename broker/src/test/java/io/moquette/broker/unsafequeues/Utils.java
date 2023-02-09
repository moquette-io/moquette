package io.moquette.broker.unsafequeues;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class Utils {

    static MappedByteBuffer createPageFile() throws IOException {
        return createPageFile(1024);
    }

    static MappedByteBuffer createPageFile(int size) throws IOException {
        final Path pageFile = File.createTempFile("test_queue", ".page").toPath();
        final OpenOption[] openOptions = {StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        FileChannel fileChannel = FileChannel.open(pageFile, openOptions);
        return fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
    }

    static MappedByteBuffer openPageFile(Path pageFile, int pageSize) throws IOException {
        final OpenOption[] openOptions = {StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING};
        FileChannel fileChannel = FileChannel.open(pageFile, openOptions);
        return fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, pageSize);
    }

    static String bufferToString(ByteBuffer buffer) {
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        return new String(data);
    }
}

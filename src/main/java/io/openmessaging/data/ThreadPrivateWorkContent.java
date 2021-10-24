package io.openmessaging.data;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ThreadPrivateWorkContent {
    public FileChannel channel;
    public ByteBuffer buffer;
    public long fileId;

    public ThreadPrivateWorkContent(FileChannel channel, int fileId) {
        this.channel = channel;
        this.buffer = ByteBuffer.allocateDirect(17 * 1024 + 9);
        this.fileId = fileId;
    }
}

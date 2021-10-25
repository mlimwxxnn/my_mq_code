package io.openmessaging.data;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class ThreadWorkContent {
    public FileChannel channel;
    public ByteBuffer buffer;
    public long fileId;
    public int groupId;

    public ThreadWorkContent(FileChannel channel, int fileId, int groupId) {
        this.channel = channel;
        this.buffer = ByteBuffer.allocateDirect(17 * 1024 + 9);
        this.fileId = fileId;
        this.groupId = groupId;
    }
}

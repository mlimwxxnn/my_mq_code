package io.openmessaging.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;

import static io.openmessaging.DefaultMessageQueueImpl.groupCount;

public class PreallocateUtil {
    public static final int preAllocateBufferSize = 100 * 4 * 1024;
    public static final long testTotalWriteFileSize = System.getProperty("os.name").contains("Windows") ? 125 * 1024 * 1024 : 125L * 1024 * 1024 * 1024;

    public static void preAllocate(FileChannel[] dataWriteChannels){
        CountDownLatch allocateCountDownLatch = new CountDownLatch(groupCount - 1);
        for (int i = 0; i < groupCount - 1; i++) {
            final int groupId = i;
            new Thread(() -> {
                try {
                    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(preAllocateBufferSize);
                    for (int j = 0; j < preAllocateBufferSize; j++) {
                        byteBuffer.put((byte) 0);
                    }
                    for (long writeTime = 0; writeTime < testTotalWriteFileSize * 1.1 / ((groupCount - 1) * preAllocateBufferSize) ; writeTime++) {
                        byteBuffer.flip();
                        dataWriteChannels[groupId].write(byteBuffer);
                    }
                    dataWriteChannels[groupId].force(false);
                    dataWriteChannels[groupId].position(0);
                    allocateCountDownLatch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        try {
            allocateCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

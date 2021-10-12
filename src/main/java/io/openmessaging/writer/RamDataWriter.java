package io.openmessaging.writer;

import io.openmessaging.data.WrappedData;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.PMEM_BLOCK_GROUP_COUNT;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class RamDataWriter {

    public static final ByteBuffer[] ramBuffer = new ByteBuffer[17];
    public static final BlockingQueue<Long>[] freeRamQueues = new LinkedBlockingQueue[17];
    private static final BlockingQueue<WrappedData> ramWrappedDataQueue = new LinkedBlockingQueue<>();

    public RamDataWriter(){
        CountDownLatch countDownLatch = new CountDownLatch(PMEM_BLOCK_GROUP_COUNT);
        for (int i = 0; i < 17; i++) {

            int finalI = i;
            new Thread(() -> {
//                ramBuffer[finalI] = ByteBuffer.allocate(1024L * (finalI + 1) * )
            }).start();
        }
    }

    public void pushWrappedData(WrappedData wrappedData){
        ramWrappedDataQueue.offer(wrappedData);
    }
}

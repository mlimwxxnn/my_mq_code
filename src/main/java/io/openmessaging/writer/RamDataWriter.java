package io.openmessaging.writer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.PMEM_BLOCK_GROUP_COUNT;

public class RamDataWriter {

    public static final BlockingQueue<Long>[] freeRamQueues = new LinkedBlockingQueue[PMEM_BLOCK_GROUP_COUNT];


    public RamDataWriter(){
        CountDownLatch countDownLatch = new CountDownLatch(PMEM_BLOCK_GROUP_COUNT);
        for (int i = 0; i < 17; i++) {
            new Thread(() -> {

            }).start();
        }
    }
}

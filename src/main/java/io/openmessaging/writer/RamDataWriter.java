package io.openmessaging.writer;

import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.data.MetaData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.PMEM_BLOCK_GROUP_COUNT;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class RamDataWriter {

    public static final ByteBuffer[] ramBuffer = new ByteBuffer[17];
    public static final BlockingQueue<Integer>[] freeRamQueues = new LinkedBlockingQueue[17];
    private static final BlockingQueue<WrappedData> ramWrappedDataQueue = new LinkedBlockingQueue<>();

    public RamDataWriter(){
        CountDownLatch countDownLatch = new CountDownLatch(PMEM_BLOCK_GROUP_COUNT);
        for (int i = 0; i < 17; i++) {

            int queueIndex = i;
            new Thread(() -> {
                ramBuffer[queueIndex] = ByteBuffer.allocate(1024 * (queueIndex + 1) * 20560); // set 20560 for allocate 3G RAM
                for (int j = 0; j < 20560; j++) {
                    freeRamQueues[queueIndex].offer(j);
                }
            }).start();
        }
    }

    public void pushWrappedData(WrappedData wrappedData){
        ramWrappedDataQueue.offer(wrappedData);
    }

    private void writeDataToRam(){
        or (int t = 0; t < PMEM_WRITE_THREAD_COUNT; t++) {
            new Thread(() -> {
                try {
                    WrappedData wrappedData;
                    ByteBuffer buf;
                    QueueInfo queueInfo;
                    MetaData meta;
                    byte[] data;
                    MemoryBlock memoryBlock;
                    PmemPageInfo pmemPageInfo;
//                    int requiredPageCount;
                    while (true) {
                        wrappedData = ramWrappedDataQueue.take();
//                        long start = System.nanoTime();
                        meta = wrappedData.getMeta();
//                        requiredPageCount = (meta.getDataLen() + PMEM_PAGE_SIZE - 1) / PMEM_PAGE_SIZE; // 向上取整
                        queueInfo = meta.getQueueInfo();

                        if ( && (pmemPageInfo = freePmemPageQueues[getFreePmemPageQueueIndex(meta.getDataLen())].poll()) != null) {
                            memoryBlock = pmemPageInfo.block;
                            buf = wrappedData.getData();
                            data = buf.array();


                            memoryBlock.copyFromArray(data, buf.position(), 0, buf.remaining());

//                            queueInfo.setDataPosInPmem(meta.getOffset(), pmemPageInfos);

                            queueInfo.setPmemBlockMemory(meta.getOffset(), pmemPageInfo);


//                            long end = System.nanoTime();
//                            System.out.printf("pmem耗时：%d", end -start);
                        }
                        meta.getCountDownLatch().countDown();

                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

package io.openmessaging.writer;

import com.intel.pmem.llpl.*;
import io.openmessaging.data.MetaData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class PmemDataWriter {

    private final Integer beginPositionRecord = 0;
    public BlockingQueue<WrappedData> pmemWrappedDataQueue = new LinkedBlockingQueue<>();
    private static TransactionalHeap heap;
    private static final Unsafe unsafe = UnsafeUtil.unsafe;


    private void initPmem(){
        boolean initialized = TransactionalHeap.exists(PMEM_ROOT + "/persistent_heap");
        heap = initialized ? TransactionalHeap.openHeap(PMEM_ROOT + "/persistent_heap") : TransactionalHeap.createHeap(PMEM_ROOT + "/persistent_heap", PMEM_HEAP_SIZE);
    }

    public PmemDataWriter() {
        initPmem();
        writeDataToPmem();
    }

    public void pushWrappedData(WrappedData wrappedData) {
        pmemWrappedDataQueue.offer(wrappedData);
    }

    public static TransactionalMemoryBlock getBlockByAllocateAndSetData(ByteBuffer data, int saveLength){
        try {
            long writeStart = System.nanoTime();

            TransactionalMemoryBlock block = heap.allocateMemoryBlock(data.remaining(), range -> {
                range.copyFromArray(data.array(), data.position(), 0, saveLength);
            });

            // 统计信息
            long writeStop = System.nanoTime();
            if (GET_WRITE_TIME_COST_INFO){
                writeTimeCostCount.addPmemTimeCost(writeStop - writeStart);
            }
            return block;
        }catch (Exception e){
            return null;
        }
    }

    private void writeDataToPmem() {
        for (int t = 0; t < PMEM_WRITE_THREAD_COUNT; t++) {
            new Thread(() -> {
                try {
                    WrappedData wrappedData;
                    QueueInfo queueInfo;
                    MetaData meta;
                    TransactionalMemoryBlock block;
                    PmemPageInfo pmemPageInfo;
                    while (true) {
                        wrappedData = pmemWrappedDataQueue.take();
                        meta = wrappedData.getMeta();
                        if ((block = getBlockByAllocateAndSetData(wrappedData.getData(), meta.getDataLen())) != null) {
                            queueInfo = meta.getQueueInfo();
                            pmemPageInfo = new PmemPageInfo(block);
                            queueInfo.setDataPosInPmem(meta.getOffset(), pmemPageInfo);
                        } else {
                            if(unsafe.compareAndSwapInt(beginPositionRecord, 12, 0, 1)) {
                                for (int i = 0; i < SSD_WRITE_THREAD_COUNT; i++) {
                                    range[i][0] = dataWriteChannels[i].size();
                                }
                            }
                        }
                        meta.getCountDownLatch().countDown();
                    }
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

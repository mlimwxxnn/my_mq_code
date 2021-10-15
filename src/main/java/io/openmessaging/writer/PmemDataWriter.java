package io.openmessaging.writer;

import com.intel.pmem.llpl.*;
import io.openmessaging.data.MetaData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class PmemDataWriter {

    public BlockingQueue<WrappedData> pmemWrappedDataQueue = new LinkedBlockingQueue<>();
    private static TransactionalHeap heap;


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

    private TransactionalMemoryBlock getBlockByAllocateAndSetData(ByteBuffer data){
        try {
            long writeStart = System.nanoTime();

            TransactionalMemoryBlock block = heap.allocateMemoryBlock(data.remaining(), range -> {
                range.copyFromArray(data.array(), data.position(), 0, data.remaining());
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
                        if ((block = getBlockByAllocateAndSetData(wrappedData.getData())) != null) {
                            queueInfo = meta.getQueueInfo();
                            pmemPageInfo = new PmemPageInfo(block);
                            queueInfo.setDataPosInPmem(meta.getOffset(), pmemPageInfo);
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

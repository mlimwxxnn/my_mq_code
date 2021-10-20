package io.openmessaging.writer;

import com.intel.pmem.llpl.*;
import io.openmessaging.data.MetaData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.ArrayQueue;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class PmemDataWriter {
    volatile boolean isNeedSaveStartChannelPosition = true;
    public BlockingQueue<WrappedData> pmemWrappedDataQueue = new LinkedBlockingQueue<>();
    private static TransactionalHeap heap;
    private static final Unsafe unsafe = UnsafeUtil.unsafe;
    static long blockAddressOffset;
    public static final AtomicLong currentAllocateSize = new AtomicLong();
    private static long maxAllocateSize = 60 * GB;

    static {
        try {
            blockAddressOffset = unsafe.objectFieldOffset(MemoryAccessor.class.getDeclaredField("directAddress"));
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }


    private void initPmem() {
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

    public static TransactionalMemoryBlock getBlockByAllocateAndSetData(ByteBuffer data, int saveLength) {
        if(currentAllocateSize.get() >= maxAllocateSize)
            return null;

        try {
            long writeStart = System.nanoTime();
            TransactionalMemoryBlock block = heap.allocateMemoryBlock(saveLength, range -> { });
            long directAddress = unsafe.getLong(block, blockAddressOffset);
            unsafe.copyMemory(data.array(), data.position() + 16, null, directAddress + 8, saveLength);

            // 统计信息
            long writeStop = System.nanoTime();
            if (GET_WRITE_TIME_COST_INFO) {
                writeTimeCostCount.addPmemTimeCost(writeStop - writeStart);
            }
            currentAllocateSize.getAndAdd(saveLength);
            return block;
        } catch (Exception e) {
            maxAllocateSize -= saveLength;
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
                        } else if (isNeedSaveStartChannelPosition) {
                            synchronized (this) {
                                if (isNeedSaveStartChannelPosition) {
                                    isNeedSaveStartChannelPosition = false;
                                    maxAllocateSize = currentAllocateSize.get();
                                    log.info("first time allocate exception, totalFileSize: {} M", getTotalFileSizeByPosition() / (1024 * 1024));
                                    for (int i = 0; i < SSD_WRITE_THREAD_COUNT; i++) {
                                        range[i][0] = dataWriteChannels[i].position();
                                    }
                                }
                            }
                        }
                        meta.getCountDownLatch().countDown();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

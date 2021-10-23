package io.openmessaging.writer;

import io.openmessaging.data.MetaData;
import io.openmessaging.data.PmemSaveSpaceData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.PmemInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.info.RamInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static io.openmessaging.data.PmemSaveSpaceData.*;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class PmemDataWriter {


    public static final BlockingQueue<PmemInfo>[] freePmemQueues = new LinkedBlockingQueue[spaceLevelCount];
    private static final BlockingQueue<WrappedData> pmemWrappedDataQueue = new LinkedBlockingQueue<>();
    private static final Unsafe unsafe = UnsafeUtil.unsafe;
    private final PmemSaveSpaceData pmemSaveSpaceData = new PmemSaveSpaceData();
    private static boolean isAllocateSpaceWhileNeed = true;

    public void init(){
        for (int queueIndex = 0; queueIndex < spaceLevelCount; queueIndex++) {
            freePmemQueues[queueIndex] = new LinkedBlockingQueue<>();
        }
    }

    public PmemDataWriter() {
        log.info("PmemDataWriter初始化");
        init();
        writeDataToPmem();
        log.info("PmemDataWriter初始化完成");
    }

    public static int getIndexByDataLength(short dataLen){
        return (dataLen + 1023) / 1024 - 1;
    }

    public void pushWrappedData(WrappedData wrappedData){
        pmemWrappedDataQueue.offer(wrappedData);
    }


    private PmemInfo getFreePmemInfo(short dataLen){
        PmemInfo pmemInfo;
        if (isAllocateSpaceWhileNeed){
            pmemInfo = pmemSaveSpaceData.allocate(dataLen);
            if (pmemInfo == null){
                isAllocateSpaceWhileNeed = false;
            }else {
                return pmemInfo;
            }
        }
        int levelIndex = RamInfo.getEnoughFreeSpaceLevelIndexByDataLen(dataLen);
        while ((pmemInfo = freePmemQueues[levelIndex].poll()) == null && levelIndex < spaceLevelCount - 1){
            levelIndex++;
        }
        return pmemInfo;
    }

    private void writeDataToPmem(){
        for(int t = 0; t < PMEM_WRITE_THREAD_COUNT; t++) {
            new Thread(() -> {
                try {
                    WrappedData wrappedData;
                    ByteBuffer buf;
                    QueueInfo queueInfo;
                    MetaData meta;
                    byte[] data;
                    short dataLen;
                    Long address;
                    PmemInfo pmemInfo;
                    while (true) {
                        wrappedData = pmemWrappedDataQueue.take();
                        meta = wrappedData.getMeta();

                        queueInfo = meta.getQueueInfo();
                        dataLen = meta.getDataLen();
                        if ((pmemInfo = getFreePmemInfo(dataLen)) != null) {
                            long writeStart = System.nanoTime(); // @

                            buf = wrappedData.getData();
                            int position = buf.position();
                            pmemChannels[pmemInfo.rLevelIndex].write(buf, pmemInfo.address);
                            buf.position(position);
                            queueInfo.setDataPosInPmem(meta.getOffset(), pmemInfo);

                            // 统计信息
                            long writeStop = System.nanoTime();  // @
                            if (GET_WRITE_TIME_COST_INFO){  // @
                                writeTimeCostCount.addPmemTimeCost(writeStop - writeStart);  // @
                            }  // @
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

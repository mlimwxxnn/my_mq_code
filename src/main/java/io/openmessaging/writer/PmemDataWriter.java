package io.openmessaging.writer;

import io.openmessaging.data.MetaData;
import io.openmessaging.data.PmemSaveSpaceData;
import io.openmessaging.data.WrappedData;
//import io.openmessaging.info.PmemInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.info.RamInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static io.openmessaging.data.PmemSaveSpaceData.*;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class PmemDataWriter {

    public static final BlockingQueue<Long>[] freePmemQueues = new LinkedBlockingQueue[spaceLevelCount];
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

    public void pushWrappedData(WrappedData wrappedData){
        pmemWrappedDataQueue.offer(wrappedData);
    }


    private long getFreePmemInfo(short dataLen){
        Long pmemInfo;
        if (isAllocateSpaceWhileNeed){
            pmemInfo = pmemSaveSpaceData.allocate(dataLen);
            if (pmemInfo == 0){
                isAllocateSpaceWhileNeed = false;
            }else {
                return (pmemInfo | ((long)dataLen << 48));
            }
        }
        int levelIndex = RamInfo.getEnoughFreeSpaceLevelIndexByDataLen(dataLen);
        int maxTryLevelIndex = Math.min(spaceLevelCount - 1, levelIndex + MAX_TRY_TIMES_WHILE_ALLOCATE_SPACE);
        while ((pmemInfo = freePmemQueues[levelIndex].poll()) == null && levelIndex < maxTryLevelIndex){
            levelIndex++;
        }
        return pmemInfo == null ? 0 : (pmemInfo | ((long)dataLen << 48));
    }

    private void writeDataToPmem(){
        for(int t = 0; t < PMEM_WRITE_THREAD_COUNT; t++) {
            new Thread(() -> {
                try {
                    WrappedData wrappedData;
                    ByteBuffer buf;
                    MetaData meta;
                    short dataLen;
                    long pmemInfo;
                    while (true) {
                        wrappedData = pmemWrappedDataQueue.take();
                        meta = wrappedData.getMeta();

                        dataLen = meta.getDataLen();
                        if ((pmemInfo = getFreePmemInfo(dataLen)) != 0) {
                            buf = wrappedData.getData();
                            pmemChannels[(byte)(pmemInfo >>> 40)].write(buf, pmemInfo & 0xff_ffff_ffffL);
                            wrappedData.posObj = pmemInfo;
                            wrappedData.state = 1;
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

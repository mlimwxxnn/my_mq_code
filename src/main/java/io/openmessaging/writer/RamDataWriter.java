package io.openmessaging.writer;

import io.openmessaging.data.MetaData;
import io.openmessaging.data.RamSaveSpaceData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.info.RamInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.*;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class RamDataWriter {

    private static RamSaveSpaceData ramSaveSpaceData;
    private static final BlockingQueue<WrappedData> ramWrappedDataQueue = new LinkedBlockingQueue<>();
    private static final Unsafe unsafe = UnsafeUtil.unsafe;
    private static boolean isAllocateSpaceWhileNeed = true;
    public static BlockingQueue<RamInfo>[] freeRamQueues = new LinkedBlockingQueue[spaceLevelCount];


    public void init(){
        ramSaveSpaceData = new RamSaveSpaceData();
        for (int i = 0; i < spaceLevelCount; i++) {
            freeRamQueues[i] = new LinkedBlockingQueue<>();
        }
    }

    public RamDataWriter() {
        log.info("RamDataWriter初始化");
        init();
        writeDataToRam();
        log.info("RamDataWriter初始化完成");
    }

    public void pushWrappedData(WrappedData wrappedData){
        ramWrappedDataQueue.offer(wrappedData);
    }

    private RamInfo getFreeRamInfo(short dataLen){
        RamInfo ramInfo;
        if (isAllocateSpaceWhileNeed){
            ramInfo = ramSaveSpaceData.allocate(dataLen);
            if (ramInfo == null){
                isAllocateSpaceWhileNeed = false;
            }else {
                return ramInfo;
            }
        }
        int levelIndex = RamInfo.getEnoughFreeSpaceLevelIndexByDataLen(dataLen);
        int maxTryLevelIndex = Math.min(spaceLevelCount - 1, levelIndex + MAX_TRY_TIMES_WHILE_ALLOCATE_SPACE);
        while ((ramInfo = freeRamQueues[levelIndex].poll())== null && levelIndex < maxTryLevelIndex){
            levelIndex++;
        }
        return ramInfo;
    }

    private void writeDataToRam(){
        for(int t = 0; t < RAM_WRITE_THREAD_COUNT; t++) {
            new Thread(() -> {
                try {
                    WrappedData wrappedData;
                    ByteBuffer buf;
                    QueueInfo queueInfo;
                    MetaData meta;
                    byte[] data;
                    short dataLen;
                    RamInfo ramInfo;
                    while (true) {
                        wrappedData = ramWrappedDataQueue.take();
                        meta = wrappedData.getMeta();

                        queueInfo = meta.getQueueInfo();
                        dataLen = meta.getDataLen();
                        if ((ramInfo = getFreeRamInfo(dataLen)) != null) {
                            buf = wrappedData.getData();
                            data = buf.array();
                            unsafe.copyMemory(data, 16 + wrappedData.getDataPosition(), ramInfo.ramObj, ramInfo.offset, wrappedData.getMeta().getDataLen());//directByteBuffer
                            wrappedData.posObj = ramInfo;
                            wrappedData.state = 2;
                            meta.getCountDownLatch().countDown();
                        } else {
                            pmemDataWriter.pushWrappedData(wrappedData);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

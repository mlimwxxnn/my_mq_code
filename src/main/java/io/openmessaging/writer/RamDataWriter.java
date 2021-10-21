package io.openmessaging.writer;

import io.openmessaging.data.MetaData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.info.RamInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.*;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class RamDataWriter {

    public static final ByteBuffer[] directRamBuffers = new ByteBuffer[17];
    public static final ByteBuffer[] heapRamBuffers = new ByteBuffer[17];
    public static final BlockingQueue<RamInfo>[] freeRamQueues = new LinkedBlockingQueue[17];
    private static final BlockingQueue<WrappedData> ramWrappedDataQueue = new LinkedBlockingQueue<>();
    private static final Unsafe unsafe = UnsafeUtil.unsafe;

    public void init(){

        int directBlocksCountPerGroup = (int)(DIRECT_CACHE_SIZE / (1024 * 153));  // 1 + 2 + 3 + ... + 17 = 153
        int heapBlocksCountPerGroup = (int)(HEAP_CACHE_SIZE / (1024 * 153));
        CountDownLatch countDownLatch = new CountDownLatch(PMEM_BLOCK_GROUP_COUNT);

        for (int i = 0; i < PMEM_BLOCK_GROUP_COUNT; i++) {
            int queueIndex = i;
            freeRamQueues[queueIndex] = new LinkedBlockingQueue<>();
            new Thread(() -> {
                try {
                    directRamBuffers[queueIndex] = ByteBuffer.allocateDirect(1024 * (queueIndex + 1) * directBlocksCountPerGroup); // 1加到17等于153, set 6853 for allocate 1G RAM
                    heapRamBuffers[queueIndex] = ByteBuffer.allocate(1024 * (queueIndex + 1) * heapBlocksCountPerGroup);
                    for (int j = 0; j < directBlocksCountPerGroup; j++) {
                        freeRamQueues[queueIndex].offer(new RamInfo(null, ((DirectBuffer) directRamBuffers[queueIndex]).address() + j * (queueIndex + 1) * 1024));
                    }
                    for (int j = 0; j < heapBlocksCountPerGroup; j++) {
                        freeRamQueues[queueIndex].offer(new RamInfo(heapRamBuffers[queueIndex].array(), 16 + j * (queueIndex + 1) * 1024));
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    System.exit(-1);
                }
                countDownLatch.countDown();
            }).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public RamDataWriter() {
        log.info("RamDataWriter初始化");
        init();
        writeDataToRam();
        log.info("RamDataWriter初始化完成");
    }

    public static int getIndexByDataLength(short dataLen){
        return (dataLen + 1023) / 1024 - 1;
    }

    public void pushWrappedData(WrappedData wrappedData){
        ramWrappedDataQueue.offer(wrappedData);
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
                        int i = getIndexByDataLength(dataLen);
                        if (!queueInfo.ramIsFull() && queueInfo.haveQueried() && (ramInfo = freeRamQueues[i].poll()) != null) {
                            long writeStart = System.nanoTime(); // @

                            buf = wrappedData.getData();
                            data = buf.array();
                            unsafe.copyMemory(data, 16 + buf.position(), ramInfo.ramObj, ramInfo.offset, buf.remaining());//directByteBuffer
                            queueInfo.setDataPosInRam(meta.getOffset(), ramInfo);
                            meta.getCountDownLatch().countDown();

                            // 统计信息
                            long writeStop = System.nanoTime();  // @
                            if (GET_WRITE_TIME_COST_INFO){  // @
                                writeTimeCostCount.addRamTimeCost(writeStop - writeStart);  // @
                            }  // @
                        }else {
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

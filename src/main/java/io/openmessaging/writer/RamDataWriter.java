package io.openmessaging.writer;

import io.openmessaging.data.MetaData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static java.lang.System.arraycopy;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class RamDataWriter {

    public static final ByteBuffer[] ramBuffers = new ByteBuffer[17];
    public static final BlockingQueue<Integer>[] freeRamQueues = new LinkedBlockingQueue[17];
    private static final BlockingQueue<WrappedData> ramWrappedDataQueue = new LinkedBlockingQueue<>();
    private static final Unsafe unsafe = UnsafeUtil.unsafe;

    public void init(){

        int blockNumsPerCache = (int)(RAM_CACHE_SIZE / (1024 * 153));  // 1 + 2 + 3 + ... + 17 = 153
        CountDownLatch countDownLatch = new CountDownLatch(PMEM_BLOCK_GROUP_COUNT);
        for (int i = 0; i < 17; i++) {
            int queueIndex = i;
            new Thread(() -> {
                ramBuffers[queueIndex] = ByteBuffer.allocateDirect(1024 * (queueIndex + 1) * blockNumsPerCache); // 1加到17等于153, set 6853 for allocate 1G RAM
                freeRamQueues[queueIndex] = new LinkedBlockingQueue<>();
                for (int j = 0; j < blockNumsPerCache; j++) {
                    freeRamQueues[queueIndex].offer(j * (queueIndex + 1) * 1024);
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
        init();
        writeDataToRam();
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
                    Integer address;
                    while (true) {
                        wrappedData = ramWrappedDataQueue.take();
//                        long start = System.nanoTime();
                        meta = wrappedData.getMeta();

                        queueInfo = meta.getQueueInfo();
                        dataLen = meta.getDataLen();
                        int i = getIndexByDataLength(dataLen);
                        if (!queueInfo.ramIsFull() && queueInfo.haveQueried() && (address = freeRamQueues[i].poll()) != null) {
                            buf = wrappedData.getData();
                            data = buf.array();

                            unsafe.copyMemory(data, 16 + buf.position(), null, address + ((DirectBuffer)ramBuffers[i]).address(), buf.remaining());//directByteBuffer
//                            arraycopy(data, buf.position(), ramBuffers[i].array(), address, buf.remaining()); //heapByteBuffer
                            queueInfo.setDataPosInRam(meta.getOffset(), address);
                            meta.getCountDownLatch().countDown();
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

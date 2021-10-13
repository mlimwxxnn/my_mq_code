package io.openmessaging.writer;

import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.data.MetaData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.PMEM_BLOCK_GROUP_COUNT;
import static io.openmessaging.DefaultMessageQueueImpl.RAM_WRITE_THREAD_COUNT;
import static io.openmessaging.writer.PmemDataWriterV2.freePmemPageQueues;
import static io.openmessaging.writer.PmemDataWriterV2.getFreePmemPageQueueIndex;
import static java.lang.System.arraycopy;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class RamDataWriter {

    public static final ByteBuffer[] ramBuffers = new ByteBuffer[17];
    public static final BlockingQueue<Integer>[] freeRamQueues = new LinkedBlockingQueue[17];
    private static final BlockingQueue<WrappedData> ramWrappedDataQueue = new LinkedBlockingQueue<>();
    private static final Unsafe unsafe = UnsafeUtil.unsafe;
    public void init(){
        CountDownLatch countDownLatch = new CountDownLatch(PMEM_BLOCK_GROUP_COUNT);
        for (int i = 0; i < 17; i++) {
            int queueIndex = i;
            new Thread(() -> {
                ramBuffers[queueIndex] = ByteBuffer.allocate(1024 * (queueIndex + 1) * 20560); // 1加到17等于153 set 20560 for allocate 3G RAM todo
                freeRamQueues[queueIndex] = new LinkedBlockingQueue<>();
                for (int j = 0; j < 20560; j++) {
                    freeRamQueues[queueIndex].offer(j);
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
                        int i = getFreePmemPageQueueIndex(dataLen);
                        if (!queueInfo.ramIsFull() && (address = freeRamQueues[i].poll()) != null) {
                            buf = wrappedData.getData();
                            data = buf.array();

                            arraycopy(data, buf.position(), ramBuffers[i].array(), address, buf.remaining());

                            queueInfo.setDataPosInRam(meta.getOffset(), address);
//                            long end = System.nanoTime();
//                            System.out.printf("ram耗时：%d", end -start);
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

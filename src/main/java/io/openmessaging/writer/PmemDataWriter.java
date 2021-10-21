package io.openmessaging.writer;

import io.openmessaging.data.MetaData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.DefaultMessageQueueImpl.*;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class PmemDataWriter {

    public static final FileChannel[] pmemChannels = new FileChannel[17];
    public static final BlockingQueue<Long>[] freePmemQueues = new LinkedBlockingQueue[17];
    private static final BlockingQueue<WrappedData> pmemWrappedDataQueue = new LinkedBlockingQueue<>();
    private static final Unsafe unsafe = UnsafeUtil.unsafe;

    public void init(){
        RandomAccessFile[] rafs = new RandomAccessFile[17];
        try {
            for (int queueIndex = 0; queueIndex < PMEM_BLOCK_GROUP_COUNT; queueIndex++) {
                freePmemQueues[queueIndex] = new LinkedBlockingQueue<>();
                rafs[queueIndex] = new RandomAccessFile("/pmem/" + queueIndex, "rw");
                pmemChannels[queueIndex] = rafs[queueIndex].getChannel();
            }

            while (true) {
                for (int i = 0; i < 17; i++) {
                    rafs[i].setLength(rafs[i].length() + 1024 * (i + 1));
                    freePmemQueues[i].offer(rafs[i].length() - 1024 * (i + 1));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public PmemDataWriter() {
        init();
        writeDataToPmem();
    }

    public static int getIndexByDataLength(short dataLen){
        return (dataLen + 1023) / 1024 - 1;
    }

    public void pushWrappedData(WrappedData wrappedData){
        pmemWrappedDataQueue.offer(wrappedData);
    }

    private void writeDataToPmem(){
        for(int t = 0; t < RAM_WRITE_THREAD_COUNT; t++) {
            new Thread(() -> {
                try {
                    WrappedData wrappedData;
                    ByteBuffer buf;
                    QueueInfo queueInfo;
                    MetaData meta;
                    byte[] data;
                    short dataLen;
                    Long address;
                    while (true) {
                        wrappedData = pmemWrappedDataQueue.take();
                        meta = wrappedData.getMeta();

                        queueInfo = meta.getQueueInfo();
                        dataLen = meta.getDataLen();
                        int i = getIndexByDataLength(dataLen);
                        if ((address = freePmemQueues[i].poll()) != null) {
                            long writeStart = System.nanoTime(); // @

                            buf = wrappedData.getData();
                            pmemChannels[i].write(buf, address);
                            queueInfo.setDataPosInPmem(meta.getOffset(), new PmemPageInfo(address, i));

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

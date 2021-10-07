package io.openmessaging;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataWriter {
    public final LinkedBlockingQueue<WrappedData> wrappedDataQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<MergedData> mergedDataQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<MergedData> freeMergedDataQueue = new LinkedBlockingQueue<>();
    final Unsafe unsafe = UnsafeUtil.unsafe;

    public DataWriter() {
        for (int i = 0; i < 50; i++) {
            freeMergedDataQueue.offer(new MergedData(ByteBuffer.allocateDirect(50 * 18 * 1024)));
        }
        mergeData();
        writeData();
    }

    public void pushWrappedData(WrappedData data) {
        wrappedDataQueue.offer(data);
    }

    void mergeData() {
        new Thread(() -> {
            int minMergeCount = 20;
            try {
                Thread.sleep(400);
                minMergeCount = Math.max(Thread.activeCount() - DefaultMessageQueueImpl.initThreadCount - 5,
                        minMergeCount);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            minMergeCount /= DefaultMessageQueueImpl.WRITE_THREAD_COUNT;


            try {
                WrappedData wrappedData;
                MergedData mergedData;
                int loopCount = 0;
                while (true) {
                    mergedData = freeMergedDataQueue.take();
                    mergedData.reset();
                    do {
                        loopCount ++;
                        for (int i = 0; i < minMergeCount; i++) {
                            wrappedData = wrappedDataQueue.poll(DefaultMessageQueueImpl.WAITE_DATA_TIMEOUT,
                                    TimeUnit.MICROSECONDS);
                            if (wrappedData != null) {
                                mergedData.putData(wrappedData);
                            } else {
                                break;
                            }
                        }
                    } while (mergedData.getCount() < 3 && loopCount < 5);
                    loopCount = 0;
                    mergedDataQueue.offer(mergedData);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    void writeData() {
        for (int i = 0; i < DefaultMessageQueueImpl.WRITE_THREAD_COUNT; i++) {
            final long writeThreadId = i;
            new Thread(() -> {
                try {
                    FileChannel dataWriteChannel = DefaultMessageQueueImpl.dataWriteChannels[(int) writeThreadId];
                    MergedData mergedData;
                    ByteBuffer mergedBuffer;
                    List<MetaData> metaList;
                    while (true) {
                        mergedData = mergedDataQueue.take();
                        mergedBuffer = mergedData.getMergedBuffer();
                        metaList = mergedData.getMetaSet();

                        // 数据写入文件
                        long pos = dataWriteChannel.position();
                        dataWriteChannel.write(mergedBuffer);
                        dataWriteChannel.force(true);

                        // 在内存中创建索引，并唤醒append的线程
                        metaList.forEach(metaData -> {
                            metaData.getQueueInfo().set(metaData.getOffset(),
                                    metaData.getOffsetInMergedBuffer() + pos,
                                    (writeThreadId << 32) | metaData.getDataLen());
                            metaData.countDownLatch.countDown();
                        });
                        freeMergedDataQueue.offer(mergedData);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

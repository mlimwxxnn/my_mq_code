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
            freeMergedDataQueue.offer(new MergedData(ByteBuffer.allocateDirect(50 * 18 * 1024))); // todo 这里也是先随便设置的，后面再调整
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
            List<WrappedData> wrappedDataList = new ArrayList<>(50);


            try {
                while (true) {
                    MergedData mergedData = freeMergedDataQueue.take();
                    mergedData.reset();
                    do {
//                        for (int i = 0; i < minMergeCount / DefaultMessageQueueImpl.WRITE_THREAD_COUNT; i++) {
//                            WrappedData wrappedData = wrappedDataQueue.poll(DefaultMessageQueueImpl
//                            .WAITE_DATA_TIMEOUT,
//                                    TimeUnit.MICROSECONDS);
//                            if (wrappedData != null) {
//                                mergedData.putData(wrappedData);
//                            } else {
//                                break;
//                            }
//                        }
                        wrappedDataQueue.drainTo(wrappedDataList);
                        mergedData.putAllData(wrappedDataList);
                        wrappedDataList.clear();
                        if (mergedData.getCount() == 0) {
                            Thread.sleep(1);
                        }
                    } while (mergedData.getCount() == 0);
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
                    List<MetaData> metaSet;
                    while (true) {
                        mergedData = mergedDataQueue.take();
                        mergedBuffer = mergedData.getMergedBuffer();
                        metaSet = mergedData.getMetaSet();

                        // 数据写入文件
                        long pos = dataWriteChannel.position();
                        dataWriteChannel.write(mergedBuffer);
                        dataWriteChannel.force(true);
                        freeMergedDataQueue.offer(mergedData);

                        // 在内存中创建索引，并唤醒append的线程
                        metaSet.forEach(metaData -> {
                            metaData.getQueueInfo().put(metaData.getOffset(),
                                    new long[]{metaData.getOffsetInMergedBuffer() + pos,
                                            (writeThreadId << 32) | metaData.getDataLen()});
                            metaData.countDownLatch.countDown();
                        });
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

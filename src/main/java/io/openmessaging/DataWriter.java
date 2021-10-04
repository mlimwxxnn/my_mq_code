package io.openmessaging;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataWriter {
    public final LinkedBlockingQueue<WrappedData> wrappedDataQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<MergedData> mergedDataQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<ByteBuffer> freeMergeBufferQueue = new LinkedBlockingQueue<>(); // 空闲的ByteBuffer
    final Unsafe unsafe = UnsafeUtil.unsafe;

    public DataWriter() {
        for (int i = 0; i < 50; i++) {
            freeMergeBufferQueue.offer(ByteBuffer.allocateDirect(50 * 18 * 1024)); // todo 这里也是先随便设置的，后面再调整
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
                System.out.println("minMergeCount: " + minMergeCount);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (true) {
                try {
                    MergedData mergedData = new MergedData(freeMergeBufferQueue.take());
                    do {
                        for (int i = 0; i < minMergeCount / DefaultMessageQueueImpl.WRITE_THREAD_COUNT; i++) {
                            WrappedData wrappedData = wrappedDataQueue.poll(DefaultMessageQueueImpl.WAITE_DATA_TIMEOUT,
                                    TimeUnit.MICROSECONDS);
                            if (wrappedData != null) {
                                mergedData.putData(wrappedData);
                            } else {
                                break;
                            }
                        }
                    } while (mergedData.getCount() == 0);
                    mergedDataQueue.offer(mergedData);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    void writeData() {
        for (int i = 0; i < DefaultMessageQueueImpl.WRITE_THREAD_COUNT; i++) {
            final long writeThreadId = i;
            new Thread(() -> {
                try {
                    FileChannel dataWriteChannel = DefaultMessageQueueImpl.dataWriteChannels[(int) writeThreadId];
                    while (true) {
                        MergedData mergedData = mergedDataQueue.take();
                        ByteBuffer mergedBuffer = mergedData.getMergedBuffer();
                        Set<MetaData> metaSet = mergedData.getMetaSet();

                        // 数据写入文件
                        long pos = dataWriteChannel.position();
                        dataWriteChannel.write(mergedBuffer);
                        dataWriteChannel.force(true);
                        freeMergeBufferQueue.offer(mergedBuffer);

                        // 在内存中创建索引，并唤醒append的线程
                        metaSet.forEach((metaData) -> {

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

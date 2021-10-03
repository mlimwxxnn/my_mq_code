package io.openmessaging;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class DataWriter {
    public final LinkedBlockingQueue<WrappedData> wrappedDataQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<MergedData> mergedDataQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<ByteBuffer> freeMergeBufferQueue = new LinkedBlockingQueue<>(); // 空闲的ByteBuffer
    final Unsafe unsafe = UnsafeUtil.unsafe;
//
//    public volatile Map<String, String> done = new ConcurrentHashMap<>();
//    public volatile Map<Thread, Integer> unparkCount = new ConcurrentHashMap<>();
//

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
                    MergedData mergedData = new MergedData(freeMergeBufferQueue.take()); // 这里只要buffer设置得足够多就不会返回null的
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
                        freeMergeBufferQueue.offer(mergedBuffer);
                        dataWriteChannel.force(true);

                        // 在内存中创建索引，并唤醒append的线程
                        metaSet.forEach((metaData) -> {
//                            byte topicId = metaData.getTopicId();
//                            int queueId = metaData.getQueueId();
//
//                            String key = topicId + "-" + queueId;
//                            done.put(key, key);

                            metaData.getQueueInfo().put(metaData.getOffset(),
                                    new long[]{metaData.getOffsetInMergedBuffer() + pos,
                                    (writeThreadId << 32) | metaData.getDataLen()});

                            metaData.countDownLatch.countDown();

//                            Integer count = unparkCount.getOrDefault(metaData.getThread(), 0);
//                            unparkCount.put(metaData.getThread(), count + 1);

                        });
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

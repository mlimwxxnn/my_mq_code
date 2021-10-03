package io.openmessaging;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
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
            while (true) {
                try {
                    MergedData mergedData = new MergedData(freeMergeBufferQueue.take()); // 这里只要buffer设置得足够多就不会返回null的
                    for (int i = 0; i < 20; i++) {  // todo 随便设置的一个数，后面可以通过一个变量来控制
                        WrappedData data = wrappedDataQueue.poll(3, TimeUnit.MILLISECONDS);// todo 这里可以用变量来控制
                        if (data != null) {
                            mergedData.putData(data);
                        } else {
                            break;
                        }
                    }
                    if (mergedData.getCount() > 0) {
                        mergedDataQueue.offer(mergedData);
                    }
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
                    FileChannel dataWriteChannel = DefaultMessageQueueImpl.dataWriteChannels[(int)writeThreadId];
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
                            byte topicId = metaData.getTopicId();
                            int queueId = metaData.getQueueId();
//
//                            String key = topicId + "-" + queueId;
//                            done.put(key, key);

                            ConcurrentHashMap<Integer, ArrayList<long[]>> topicInfo =
                                    DefaultMessageQueueImpl.metaInfo.get(topicId);
                            if (topicInfo == null) {
                                topicInfo = new ConcurrentHashMap<>();
                                DefaultMessageQueueImpl.metaInfo.put(topicId, topicInfo);
                            }
                            ArrayList<long[]> queueInfo = topicInfo.get(queueId);
                            if (queueInfo == null) {
                                queueInfo = new ArrayList<>(5000);
                                topicInfo.put(queueId, queueInfo);
                            }
                            queueInfo.add(new long[]{metaData.getOffsetInMergedBuffer() + pos,
                                    (writeThreadId << 32) | metaData.getDataLen()});

//                            unsafe.unpark(metaData.getThread());
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

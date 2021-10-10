package io.openmessaging.writer;

import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.data.MergedData;
import io.openmessaging.data.MetaData;
import io.openmessaging.data.WrappedData;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.openmessaging.DefaultMessageQueueImpl.DATA_INFORMATION_LENGTH;
import static io.openmessaging.DefaultMessageQueueImpl.writtenDataSize;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class SsdDataWriter {
    public final BlockingQueue<WrappedData> ssdWrappedDataQueue = new LinkedBlockingQueue<>(50);
    public final BlockingQueue<MergedData> mergedDataQueue = new LinkedBlockingQueue<>(50);
    public final BlockingQueue<MergedData> freeMergedDataQueue = new LinkedBlockingQueue<>(50);

    public SsdDataWriter() {
        for (int i = 0; i < 50; i++) {
            freeMergedDataQueue.offer(new MergedData(ByteBuffer.allocateDirect(50 * 18 * 1024)));
        }
        mergeData();
        writeData();
    }

    public void pushWrappedData(WrappedData data) {
        ssdWrappedDataQueue.offer(data);
    }

    void mergeData() {
        new Thread(() -> {
//            int minMergeCount = 20;
//            try {
//                Thread.sleep(400);
//                minMergeCount = Math.max(Thread.activeCount() - DefaultMessageQueueImpl.initThreadCount - 5,
//                        minMergeCount);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            minMergeCount /= DefaultMessageQueueImpl.WRITE_THREAD_COUNT;


            try {
                WrappedData wrappedData;
                MergedData mergedData;
                int loopCount = 0;
                while (true) {
                    mergedData = freeMergedDataQueue.take();
                    mergedData.reset();
                    do {
                        loopCount ++;
                        for (int i = 0; i < 7; i++) {
                            wrappedData = ssdWrappedDataQueue.poll(DefaultMessageQueueImpl.WAITE_DATA_TIMEOUT,
                                    TimeUnit.MICROSECONDS);
                            if (wrappedData != null) {
                                mergedData.putData(wrappedData);
                            } else {
                                break;
                            }
                        }
                    } while (mergedData.getCount() == 0 || mergedData.getCount() < 3 && loopCount < 10);
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
//                        long start = System.nanoTime();
                        mergedBuffer = mergedData.getMergedBuffer();
                        metaList = mergedData.getMetaList();

                        // 数据写入文件
                        long pos = dataWriteChannel.position();
                        dataWriteChannel.write(mergedBuffer);
                        dataWriteChannel.force(true);

                        // 在内存中创建索引，并唤醒append的线程
                        metaList.forEach(metaData -> {
                            metaData.getQueueInfo().setDataPosInFile(metaData.getOffset(),
                                    metaData.getOffsetInMergedBuffer() + pos,
                                    (writeThreadId << 32) | metaData.getDataLen());
                            metaData.getCountDownLatch().countDown();
                        });
                        writtenDataSize.getAndAdd(mergedBuffer.limit() - (long) DATA_INFORMATION_LENGTH * mergedData.getCount());

                        freeMergedDataQueue.offer(mergedData);
//                        long end = System.nanoTime();
//                        System.out.printf("ssd写入%d个耗时：%d\n", mergedData.getCount(), end - start);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

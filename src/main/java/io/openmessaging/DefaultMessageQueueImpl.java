package io.openmessaging;

import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import io.openmessaging.data.CacheHitCountData;
import io.openmessaging.data.GetRangeTaskData;
import io.openmessaging.data.TimeCostCountData;
import io.openmessaging.data.WrappedData;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.reader.DataReader;
import io.openmessaging.writer.ReloadData;
import io.openmessaging.writer.PmemDataWriter;
import io.openmessaging.writer.RamDataWriter;
import io.openmessaging.writer.SsdDataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


@SuppressWarnings("ResultOfMethodCallIgnored")
public class DefaultMessageQueueImpl extends MessageQueue {

    public static final boolean GET_CACHE_HIT_INFO = true;
    public static final boolean GET_WRITE_TIME_COST_INFO = true;
    public static final boolean GET_READ_TIME_COST_INFO = true;
    public static final int PMEM_BLOCK_GROUP_COUNT = 17;
    public static final Logger log = LoggerFactory.getLogger("myLogger");
    public static final long GB = 1024L * 1024L * 1024L;
    public static final long MB = 1024L * 1024L;
    public static File DISC_ROOT;
    public static File PMEM_ROOT;

    public static final int DATA_INFORMATION_LENGTH = 9;
    public static final long KILL_SELF_TIMEOUT = 20 * 60;  // seconds
    public static final long WAITE_DATA_TIMEOUT = 300;  // 微秒
    public static final int SSD_WRITE_THREAD_COUNT = 5;
    public static final int SSD_MERGE_THREAD_COUNT = 1;
    public static final int READ_THREAD_COUNT = 20;
    public static final int PMEM_WRITE_THREAD_COUNT = 8;
    public static final int RAM_WRITE_THREAD_COUNT = 8;
    public static final long RAM_CACHE_SIZE = 1900 * MB;
    public static final long PMEM_HEAP_SIZE = 60 * GB;
//     public static final long PMEM_HEAP_SIZE = 20 * MB;
    public static long roughWrittenDataSize = 0;

    public static AtomicInteger topicCount = new AtomicInteger();
    static private final ConcurrentHashMap<String, Byte> topicNameToTopicId = new ConcurrentHashMap<>();
    public static volatile ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo;
    public static volatile Map<Thread, GetRangeTaskData> getRangeTaskMap = new ConcurrentHashMap<>();
    public static final FileChannel[] dataWriteChannels = new FileChannel[SSD_WRITE_THREAD_COUNT];
    public static SsdDataWriter ssdDataWriter;
    public static DataReader dataReader;
    public static int initThreadCount = 0;
    public static PmemDataWriter pmemDataWriter;
    public static RamDataWriter ramDataWriter;
    public static final long[][] range = new long[SSD_WRITE_THREAD_COUNT][2];


    public static CacheHitCountData hitCountData;
    public static TimeCostCountData writeTimeCostCount;
    public static TimeCostCountData readTimeCostCount;
    private static long constructFinishTime;


    public static void init() {
        try {
            metaInfo = new ConcurrentHashMap<>(100);
            dataReader = new DataReader();
            for (int i = 0; i < SSD_WRITE_THREAD_COUNT; i++) {
                File file = new File(DISC_ROOT, "data-" + i);
                File parentFile = file.getParentFile();
                if (!parentFile.exists()) {
                    parentFile.mkdirs();
                }
                if (!file.exists()) {
                    file.createNewFile();
                }
                dataWriteChannels[i] = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
            }
            // 恢复阶段不实例化写
            if (getTotalFileSize() > 0){
                return;
            }
            ssdDataWriter = new SsdDataWriter();
            pmemDataWriter = new PmemDataWriter();
//            ramDataWriter = new RamDataWriter();
            new Thread(() -> {
                try {
                    while (roughWrittenDataSize < 75 * GB){
                        Thread.sleep(10 * 1000);
                        roughWrittenDataSize = getTotalFileSizeByPosition();
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }).start();

            // 以下为debug或者打印日志信息初始化的区域
            //***********************************************************************
            if(GET_WRITE_TIME_COST_INFO){
                writeTimeCostCount = new TimeCostCountData("write");
            }
            if (GET_CACHE_HIT_INFO){
                hitCountData = new CacheHitCountData();
                new Thread(() -> {
                    try {
                        // 有查询后再打印
                        while (roughWrittenDataSize < 75 * GB){
                            Thread.sleep(10 * 1000);
                        }
                        while (true){
                            // 每10s打印一次
                            Thread.sleep(10 * 1000);
                            log.info(hitCountData.getHitCountInfo());
                        }
                    }catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static long getTotalFileSize() {
        long fileSize = 0;
        for (FileChannel dataWriteChannel : dataWriteChannels) {
            try {
                fileSize += dataWriteChannel.size();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileSize;
    }

    public static long getTotalFileSizeByPosition() {
        long fileSize = 0;
        for (FileChannel dataWriteChannel : dataWriteChannels) {
            try {
                fileSize += dataWriteChannel.position();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileSize;
    }

    public static GetRangeTaskData getTask(Thread thread) {
        return getRangeTaskMap.computeIfAbsent(thread, k -> new GetRangeTaskData());
    }

    public static void killSelf(long timeout) {
        if (timeout <= 0) {
            return;
        }
        new Thread(() -> {
            try {
                Thread.sleep(timeout * 1000);
                long writtenSize = getTotalFileSizeByPosition() / (1024 * 1024);
                System.out.printf("kill self(written: %d M)%n", writtenSize);
                System.exit(-1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void testLlpl(){
        byte[] data = new byte[8 * 1024];
        int count = 0;
        TransactionalHeap heap = TransactionalHeap.createHeap("/pmem" + "/persistent_heap", PMEM_HEAP_SIZE);
        TransactionalMemoryBlock block;
        do {
            try {
                block = heap.allocateMemoryBlock(data.length, range -> {
                    range.copyFromArray(data, 0, 0, data.length);
                });
            }catch (Exception e){
                block = null;
            }
            count ++;
        }while (block != null);
        System.out.printf("final block count: %d", count);
        System.exit(-1);

    }

    public DefaultMessageQueueImpl() {
        log.info("DefaultMessageQueueImpl 开始执行构造函数");
        DISC_ROOT = System.getProperty("os.name").contains("Windows") ? new File("./essd") : new File("/essd");
        PMEM_ROOT = System.getProperty("os.name").contains("Windows") ? new File("./pmem") : new File("/pmem");

        init();

        powerFailureRecovery(metaInfo);
        initThreadCount = Thread.activeCount();
        log.info("DefaultMessageQueueImpl 构造函数执行完成");
        killSelf(KILL_SELF_TIMEOUT);
        constructFinishTime = System.currentTimeMillis();
    }

    public void powerFailureRecovery(ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo) {
        for (int id = 0; id < dataWriteChannels.length; id++) {
            FileChannel channel = dataWriteChannels[id];
            ByteBuffer readBuffer = ByteBuffer.allocate(DATA_INFORMATION_LENGTH);
            byte topicId;
            short queueId;
            short dataLen = 0;
            int offset;
            try {
                ConcurrentHashMap<Short, QueueInfo> topicInfo;
                QueueInfo queueInfo;
                long dataFilesize = channel.size();
                while (channel.position() + dataLen < dataFilesize) {
                    channel.position(channel.position() + dataLen); // 跳过数据部分只读取数据头部的索引信息
                    readBuffer.clear();
                    channel.read(readBuffer);
                    readBuffer.flip();
                    topicId = readBuffer.get();
                    queueId = readBuffer.getShort();
                    dataLen = readBuffer.getShort();
                    offset = readBuffer.getInt();

                    topicInfo = metaInfo.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>(2000));
                    queueInfo = topicInfo.computeIfAbsent(queueId, k -> new QueueInfo());
                    long groupIdAndDataLength = (((long) id) << 32) | dataLen;
                    queueInfo.setDataPosInFile(offset, channel.position(), groupIdAndDataLength);
                }
            } catch (Exception e) {
                System.out.println("ignore exception while rectory");
            }
        }
    }


    @SuppressWarnings("ConstantConditions")
    @Override
    public long append(String topic, int queueId, ByteBuffer data) {

        haveAppended = true;
        Byte topicId = getTopicId(topic, true);

        ConcurrentHashMap<Short, QueueInfo> topicInfo = metaInfo.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>(2000));
        QueueInfo queueInfo = topicInfo.computeIfAbsent((short) queueId, k -> new QueueInfo());
        int offset = queueInfo.size();

        WrappedData wrappedData = new WrappedData(topicId, (short) queueId, data, offset, queueInfo);
        ssdDataWriter.pushWrappedData(wrappedData);

        try {
            if(roughWrittenDataSize > 20 * GB){
//                ramDataWriter.pushWrappedData(wrappedData);
                pmemDataWriter.pushWrappedData(wrappedData);
            } else {
                wrappedData.getMeta().getCountDownLatch().countDown();
            }
            wrappedData.getMeta().getCountDownLatch().await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return offset;
    }

    /**
     * 创建或并返回topic对应的topicId
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static Byte getTopicId(String topic, boolean isCreateNew) {
        Byte topicId = topicNameToTopicId.get(topic);
        if (topicId == null) {
            File topicIdFile = new File(DISC_ROOT, "topic/" + topic);
            try {
                if (!topicIdFile.exists()) {
                    if (!isCreateNew) {
                        return null;
                    }
                    if (!topicIdFile.getParentFile().exists()) {
                        topicIdFile.getParentFile().mkdir();
                    }
                    // 文件不存在，这是一个新的Topic，保存topic名称到topicId的映射，文件名为topic，内容为id
                    topicNameToTopicId.put(topic, topicId = (byte) topicCount.getAndIncrement());
                    FileOutputStream fos = new FileOutputStream(topicIdFile);
                    fos.write(topicId);
                    fos.flush();
                    fos.close();
                } else {
                    // 文件存在，topic不在内存，从文件恢复
                    FileInputStream fis = new FileInputStream(topicIdFile);
                    topicId = (byte) fis.read();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return topicId;
    }


    boolean haveAppended = false;
    volatile boolean isFirstStageGoingOn = true;
    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {

        if (isFirstStageGoingOn) {
            synchronized (this) {
                if(isFirstStageGoingOn){
                    isFirstStageGoingOn = false;
                    if (GET_READ_TIME_COST_INFO){
                        readTimeCostCount = new TimeCostCountData("read");
                    }
                    log.info("第一阶段结束 cost: {}", System.currentTimeMillis() - constructFinishTime);

                    for (int i = 0; i < SSD_WRITE_THREAD_COUNT; i++) {
                        try {
                            range[i][1] = dataWriteChannels[i].size();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    new ReloadData();
//                    System.exit(-1);
                }
            }
        }

        if(!haveAppended){
            System.exit(-1);
        }

        GetRangeTaskData task = getTask(Thread.currentThread());
        task.setGetRangeParameter(topic, queueId, offset, fetchNum);
        dataReader.pushTask(task);
        try {
            task.getCountDownLatch().await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (GET_CACHE_HIT_INFO){
            hitCountData.addTotalQueryCount(task.getResult().size());
        }
        return task.getResult();
    }

    public static void main(String[] args) throws InterruptedException {
        final int threadCount = 40;
        final int topicCountPerThread = 3;  // threadCount * topicCountPerThread <= 100
        final int queueIdCountPerTopic = 5;
        final int writeTimesPerQueueId = 3;
        ByteBuffer[][][] buffers = new ByteBuffer[threadCount][topicCountPerThread][queueIdCountPerTopic];
        DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();

        // 初始化源 buffer
        for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
            for (int topicIndex = 0; topicIndex < topicCountPerThread; topicIndex++) {
                for (int queueIndex = 0; queueIndex < queueIdCountPerTopic; queueIndex++) {
                    ByteBuffer buffer = ByteBuffer.allocate(8);
                    buffer.putLong(queueIndex);
                    buffer.flip();
                    buffers[threadIndex][topicIndex][queueIndex] = buffer;
                }
            }
        }

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int threadIndex = i;
            threads[i] = new Thread(() -> {
                for (int topicIndex = 0; topicIndex < topicCountPerThread; topicIndex++) {
                    String topic = threadIndex + "-" + topicIndex;
                    for (int queueIndex = 0; queueIndex < queueIdCountPerTopic; queueIndex++) {
                        for (int t = 0; t < writeTimesPerQueueId; t++) {
                            buffers[threadIndex][topicIndex][queueIndex].putInt(t);
                            buffers[threadIndex][topicIndex][queueIndex].position(0);
                            mq.append(topic, queueIndex, buffers[threadIndex][topicIndex][queueIndex]);
                        }
                    }
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }

        Map<Integer, ByteBuffer> res = mq.getRange("10-1", 3, 0, 100);
        System.out.println(res.size());
    }
}

package io.openmessaging;

import io.openmessaging.data.*;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.writer.PmemDataWriter;
import io.openmessaging.writer.RamDataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.util.UnsafeUtil.unsafe;

/**
 * 优化点：
 * 1. 内存占用优化（包括：查过的数据，内粗不再继续存储它的索引； hashmap -> arraylist）
 * **/
@SuppressWarnings("ResultOfMethodCallIgnored")
public class DefaultMessageQueueImpl extends MessageQueue {

    public static final Logger log = LoggerFactory.getLogger("myLogger");
    public static final long GB = 1024L * 1024L * 1024L;
    public static final long MB = 1024L * 1024L;
    public static File DISC_ROOT;
    public static File PMEM_ROOT;

    public static final ThreadLocal<ThreadWorkContent> threadWorkContentMap = new ThreadLocal<>();
    public static final AtomicInteger totalThreadCount = new AtomicInteger();
    public static final int THREAD_COUNT_PER_GROUP = 10;
    public static final int MAX_THREAD_COUNT = 50;
    public static final int groupCount = MAX_THREAD_COUNT / THREAD_COUNT_PER_GROUP;
    public static volatile ByteBuffer[] groupBuffers = new ByteBuffer[groupCount];
    public static final CyclicBarrier[] cyclicBarriers = new CyclicBarrier[groupCount];
    public static final long[] groupBufferBasePos = new long[groupCount];
    public static final AtomicInteger[] groupBufferWritePos = new AtomicInteger[groupCount];


    public static final int DATA_INFORMATION_LENGTH = 9;
    public static final int PMEM_WRITE_THREAD_COUNT = 8;
    public static final int RAM_WRITE_THREAD_COUNT = 8;
    public static final long DIRECT_CACHE_SIZE = 1900 * MB;
    public static final long HEAP_CACHE_SIZE = 2048 * MB;
    public static final int RAM_SPACE_LEVEL_GAP = 200; // B
    public static final int spaceLevelCount = (17 * 1024 + RAM_SPACE_LEVEL_GAP - 1) / RAM_SPACE_LEVEL_GAP;
    public static final int MAX_TRY_TIMES_WHILE_ALLOCATE_SPACE = 5;
    public static final long PMEM_CACHE_SIZE = 60 * GB;
    public static final long MAX_MESSAGE_FILE_SIZE = 125 * GB;
//     public static final long PMEM_HEAP_SIZE = 20 * MB;


    public static AtomicInteger topicCount = new AtomicInteger();
    static private final ConcurrentHashMap<String, Byte> topicNameToTopicId = new ConcurrentHashMap<>();
    public static volatile ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo;
    public static volatile Map<Thread, GetRangeTaskData> getRangeTaskMap = new ConcurrentHashMap<>();
    public static final FileChannel[] dataWriteChannels = new FileChannel[groupCount + MAX_THREAD_COUNT];//

    public static PmemDataWriter pmemDataWriter;
    public static RamDataWriter ramDataWriter;

    public static AtomicInteger getRangeCount = new AtomicInteger();

//    static long allocateWriteTime = PreallocateSpeedTest.preAllocate();
//    static long noAllocateWriteTime = PreallocateSpeedTest.noAllocate();


    public static void init() {
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            log.info("getRangeCount:{}", getRangeCount);
//            log.info("{},{}", allocateWriteTime, noAllocateWriteTime);
        }));

        try {
            for (int i = 0; i < groupCount; i++) {
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(THREAD_COUNT_PER_GROUP * 18 * 1024);
                final int groupId = i;
                groupBuffers[groupId] = byteBuffer;
                groupBufferWritePos[groupId] = new AtomicInteger();
                groupBufferBasePos[groupId] = ((DirectBuffer) byteBuffer).address();
                cyclicBarriers[groupId] = new CyclicBarrier(THREAD_COUNT_PER_GROUP, () -> {
                    try {
                        groupBuffers[groupId].position(0);
                        groupBuffers[groupId].limit(groupBufferWritePos[groupId].get());
                        dataWriteChannels[groupId].write(groupBuffers[groupId]);
                        dataWriteChannels[groupId].force(true);
                        groupBufferWritePos[groupId].set(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

            metaInfo = new ConcurrentHashMap<>(100);
            for (int i = 0; i < dataWriteChannels.length; i++) {
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

            // ssd预先填0

            // 恢复阶段不实例化写
            if (getTotalFileSize() > 0){
                return;
            }
            pmemDataWriter = new PmemDataWriter();
            ramDataWriter = new RamDataWriter();
        }catch(Exception e){
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

    public static GetRangeTaskData getTask(Thread thread) {
        return getRangeTaskMap.computeIfAbsent(thread, k -> new GetRangeTaskData());
    }

    public DefaultMessageQueueImpl() {
        log.info("DefaultMessageQueueImpl 开始执行构造函数");
        DISC_ROOT = System.getProperty("os.name").contains("Windows") ? new File("d:/essd") : new File("/essd");
        PMEM_ROOT = System.getProperty("os.name").contains("Windows") ? new File("./pmem") : new File("/pmem");

        init();
        // 断电后数据恢复
        powerFailureRecovery(metaInfo);
        log.info("DefaultMessageQueueImpl 构造函数执行完成");
    }

    public void powerFailureRecovery(ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo) {
        if (getTotalFileSize() == 0){
            return;
        }
        String[] topicNames = new String[100];
        File topicIdFile = new File(DISC_ROOT, "topic");
        try {
            for (File topicFile : topicIdFile.listFiles()) {
                String topicName = topicFile.getName();
                FileInputStream fis = new FileInputStream(topicFile);
                int topicId = (byte) fis.read();
                topicNames[topicId] = topicName;
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        log.info("开始执行断电恢复函数，totalFileSize: {}", getTotalFileSize());
        // 分组文件以及私有文件
        for (int id = 0; id < dataWriteChannels.length; id++) {
            ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> localMetaInfo = new ConcurrentHashMap<>(10);

            ByteBuffer readBuffer = ByteBuffer.allocate(DATA_INFORMATION_LENGTH);
            FileChannel channel = dataWriteChannels[id];
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
                    queueId = Short.reverseBytes(readBuffer.getShort());
                    dataLen = Short.reverseBytes(readBuffer.getShort());
                    offset = Integer.reverseBytes(readBuffer.getInt());
//                    topicInfo = metaInfo.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>(2000));
                    topicInfo = localMetaInfo.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>(2000));
                    queueInfo = topicInfo.computeIfAbsent(queueId, k -> new QueueInfo());
                    long groupIdAndDataLength = (((long) id) << 32) | dataLen;
                    queueInfo.setDataPosInFile(offset, channel.position(), groupIdAndDataLength);
                }
            } catch (Exception e) {
                System.out.println("ignore exception while rectory");
            }

            metaInfo.putAll(localMetaInfo);

            try {
                log.info("channel: {}, channel.size(): {}, recoveryTopicCount: {}", id, channel.size(), localMetaInfo.size());
            } catch (IOException e) {
                e.printStackTrace();
            }
            for (Byte localTopicId : localMetaInfo.keySet()) {
                ConcurrentHashMap<Short, QueueInfo> topicMap = localMetaInfo.get(localTopicId);
                log.info("topic: {}, queueNums: {}", topicNames[localTopicId], topicMap.size());
                for (Short localQueueId : topicMap.keySet()) {
                    QueueInfo queueInfo = topicMap.get(localQueueId);
                    System.out.printf("queueId: %d, \tqueueSize: %d\n", localQueueId, queueInfo.size());
                }
            }
            System.out.println("************************************************************");
        }
    }

    // 每个线程私有，barrier broken后用私有（不会和其他线程共同拥有）的channel单个落盘数据
    private ThreadWorkContent getWorkContent(){
        ThreadWorkContent threadWorkContent = threadWorkContentMap.get();
        if (threadWorkContent == null){
            int threadId = totalThreadCount.getAndIncrement();
            int groupId = threadId / THREAD_COUNT_PER_GROUP;
            // 线程私有的channel，用来写单条数据到ssd
            int fileId = threadId + groupCount;
            FileChannel channel = dataWriteChannels[fileId];
            threadWorkContent = new ThreadWorkContent(channel, fileId, groupId);
            threadWorkContentMap.set(threadWorkContent);
        }
        return threadWorkContent;
    }


    @SuppressWarnings("ConstantConditions")
    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        ThreadWorkContent workContent = getWorkContent();
        int groupId = workContent.groupId;
        WrappedData wrappedData = workContent.wrappedData;
        short dataLen = (short) data.remaining();
        int dataPosition = data.position();

        Byte topicId = getTopicId(topic, true);
        ConcurrentHashMap<Short, QueueInfo> topicInfo = metaInfo.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>(2000));
        QueueInfo queueInfo = topicInfo.computeIfAbsent((short) queueId, k -> new QueueInfo());
        int offset = queueInfo.size();

        wrappedData.setWrapInfo(data, offset, queueInfo);
        // 写缓存
        ramDataWriter.pushWrappedData(wrappedData);
//        pmemDataWriter.pushWrappedData(wrappedData);

        try {
            appendSsdByGroup(groupId, topicId, queueId, offset, queueInfo, data, dataLen, dataPosition);
            wrappedData.getMeta().getCountDownLatch().await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return offset;
    }

    // 聚合写入
    public void appendSsdByGroup(int groupId, byte topicId, int queueId, int offset, QueueInfo queueInfo, ByteBuffer data, short dataLen, int dataPosition) {
        int currentBufferPos = groupBufferWritePos[groupId].getAndAdd( 9 + dataLen);
        long currentBufferAddress = groupBufferBasePos[groupId] + currentBufferPos;
        unsafe.putByte(currentBufferAddress, topicId);
        unsafe.putShort(currentBufferAddress + 1, (short)queueId);
        unsafe.putShort(currentBufferAddress + 3, dataLen);
        unsafe.putInt(currentBufferAddress + 5, offset);
        // 放入数据本体
        unsafe.copyMemory(data.array(), 16 + dataPosition, null, currentBufferAddress + 9, dataLen);
        try {
            queueInfo.setDataPosInFile(offset, dataWriteChannels[groupId].position() + currentBufferPos + 9, (((long) groupId) << 32) | dataLen);
            cyclicBarriers[groupId].await(5, TimeUnit.SECONDS);
        } catch ( IOException | InterruptedException | BrokenBarrierException | TimeoutException e) {
//            log.info("cyclicBarrier timeout handle groupId: {}", groupId);
            // 超时等异常后把数据写入自己的channel
            groupBufferWritePos[groupId].set(0);
            appendSsdBySelf(getWorkContent(), topicId, queueId, offset, queueInfo, data, dataLen, dataPosition);
        }
    }

    // 单条写入
    public void appendSsdBySelf(ThreadWorkContent workContent, byte topicId, int queueId, int offset, QueueInfo queueInfo, ByteBuffer data, short dataLen, int dataPosition) {
        FileChannel channel = workContent.channel;
        ByteBuffer buffer = workContent.buffer;
        long currentBufferAddress = ((DirectBuffer) buffer).address();

        unsafe.putByte(currentBufferAddress, topicId);
        unsafe.putShort(currentBufferAddress + 1, (short)queueId);
        unsafe.putShort(currentBufferAddress + 3, dataLen);
        unsafe.putInt(currentBufferAddress + 5, offset);
        // 放入数据本体
        unsafe.copyMemory(data.array(), 16 + dataPosition, null, currentBufferAddress + 9, dataLen);
        try {
            queueInfo.setDataPosInFile(offset, channel.position() + 9, (workContent.fileId << 32) | dataLen);
            buffer.position(0);
            buffer.limit(dataLen + 9);
            channel.write(buffer);
            channel.force(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
                        topicIdFile.getParentFile().mkdirs();
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



    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        GetRangeTaskData task = getTask(Thread.currentThread());
        task.setGetRangeParameter(topic, queueId, offset, fetchNum);
        task.queryData();
        getRangeCount.getAndIncrement();
        return task.getResult();
    }


    // 本地调试
    public static final int threadCount = 42;
    public static final int topicCountPerThread = 2;  // threadCount * topicCountPerThread <= 100
    public static final int queueIdCountPerTopic = 50;
    public static final int writeTimesPerQueueId = 10;
    public static void main(String[] args) throws InterruptedException {
        for (File file : new File("d:/essd").listFiles()) {
            if(file.isFile()){
                file.delete();
            }else{
                for (File listFile : file.listFiles()) {
                    if (listFile.isFile()){
                        listFile.delete();
                    }
                }
            }
        }

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
                            buffers[threadIndex][topicIndex][queueIndex].position(0);
                            buffers[threadIndex][topicIndex][queueIndex].putInt(t);
                            buffers[threadIndex][topicIndex][queueIndex].position(0);
                            mq.append(topic, queueIndex, buffers[threadIndex][topicIndex][queueIndex]);
                        }
                    }
                    System.out.printf("topic %s finish\n", topic);
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }


        // ***********************************************
        Thread[] threads1 = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int threadIndex = i;
            threads1[i] = new Thread(() -> {
                for (int topicIndex = 0; topicIndex < topicCountPerThread; topicIndex++) {
                    String topic = threadIndex + "-" + topicIndex;
                    for (int queueIndex = 0; queueIndex < queueIdCountPerTopic; queueIndex++) {
                        for (int t = 0; t < writeTimesPerQueueId; t++) {
                            Map<Integer, ByteBuffer> res = mq.getRange(topic, queueIndex, t, 1);
                            ByteBuffer byteBuffer = res.get(0);
                            if (topicIndex == 0 && queueIndex == 3 && t == 158 && "9-0".equals(topic)){
                                int a = 1;
                            }
                            if (byteBuffer.remaining() != 8){
                                System.out.println(String.format("remaining error: %d: %d: %d, topic: %s, remaining: %d", topicIndex, queueIndex, t, topic, byteBuffer.remaining()));
                            }
                            if (byteBuffer.getInt(0) != t || byteBuffer.getInt(4) != queueIndex){
                                System.out.println(String.format("data error: %d: %d: %d topic: %s, data: {%d, %d}", topicIndex, queueIndex, t, topic, byteBuffer.getInt(), byteBuffer.getInt()));
//                                System.exit(-1);
                            }
                        }
                    }
                }
            });
            threads1[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads1[i].join();
        }
        System.out.println("query done.");


//        mq.append("big_data", 1, ByteBuffer.allocate(17*1024));
//        ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo = mq.metaInfo;
//        for (Byte topicId : metaInfo.keySet()) {
//            ConcurrentHashMap<Short, QueueInfo> topicInfo = metaInfo.get(topicId);
//            for (Short queueId : topicInfo.keySet()) {
//                QueueInfo queueInfo = topicInfo.get(queueId);
//                for (int offset = 0; offset < queueInfo.size(); offset++) {
//                    long[] p = queueInfo.getDataPosInFile(offset);
//                    short dataLen = (short) p[1];
//                    if (dataLen != 8){
//                        int a = 1;
//                    }
//                }
//            }
//        }
    }
}

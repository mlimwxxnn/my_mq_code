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
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.util.UnsafeUtil.unsafe;

/**
 * 优化点：
 * 1. 内存占用优化（包括：查过的数据，内粗不再继续存储它的索引； hashmap -> arraylist）
 * 2. pmem使用到 62G
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
    public static final int groupCount = 4;
    public static volatile ByteBuffer[] groupBuffers = new ByteBuffer[groupCount * 2];
    public static final CyclicBarrier[] cyclicBarriers = new CyclicBarrier[groupCount * 2];
    public static final long[] groupBufferBasePos = new long[groupCount * 2];
    public static final int[] awaitThreadCountLimits = new int[groupCount * 2];
    public static final AtomicInteger[] groupBufferWritePos = new AtomicInteger[groupCount * 2];
    public static final AtomicInteger[] groupWaitThreadCount = new AtomicInteger[groupCount * 2];


    public static final int DATA_INFORMATION_LENGTH = 9;
    public static long KILL_SELF_TIMEOUT = 15 * 60;  // seconds
    public static final int PMEM_WRITE_THREAD_COUNT = 8;
    public static final int RAM_WRITE_THREAD_COUNT = 8;
    public static final long DIRECT_CACHE_SIZE = /*direct*/1900/*direct*/ * MB;
    public static final long HEAP_CACHE_SIZE = /*heap*/2048/*heap*/ * MB;
    public static final int RAM_SPACE_LEVEL_GAP = /*gap*/200/*gap*/; // B
    public static final int spaceLevelCount = (17 * 1024 + RAM_SPACE_LEVEL_GAP - 1) / RAM_SPACE_LEVEL_GAP;
    public static final int MAX_TRY_TIMES_WHILE_ALLOCATE_SPACE = 5;
    public static final long PMEM_CACHE_SIZE = 60 * GB;
//     public static final long PMEM_HEAP_SIZE = 20 * MB;


    public static AtomicInteger topicCount = new AtomicInteger();
    static private final ConcurrentHashMap<String, Byte> topicNameToTopicId = new ConcurrentHashMap<>();
    public static volatile ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo;
    public static volatile Map<Thread, GetRangeTaskData> getRangeTaskMap = new ConcurrentHashMap<>();
    public static final FileChannel[] dataWriteChannels = new FileChannel[groupCount * 2 + 50];//

    public static PmemDataWriter pmemDataWriter;
    public static RamDataWriter ramDataWriter;


    public static void init() {
        try {
            for (int i = 0; i < groupCount * 2; i++) {
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(10 * 18 * 1024);
                groupBuffers[i] = byteBuffer;
                groupBufferWritePos[i] = new AtomicInteger();
                groupWaitThreadCount[i] = new AtomicInteger();
                groupBufferBasePos[i] = ((DirectBuffer) byteBuffer).address();
                if (i < groupCount){
                    cyclicBarriers[i] = new CyclicBarrier(10);
                    awaitThreadCountLimits[i] = 10;
                }else {
                    // 重分组的barrier设置
                    cyclicBarriers[i] = new CyclicBarrier(8);
                    awaitThreadCountLimits[i] = 8;
                }
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
            // 恢复阶段不实例化写
            if (getTotalFileSize() > 0){
                return;
            }
            pmemDataWriter = new PmemDataWriter();
            ramDataWriter = new RamDataWriter();
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
                log.info("kill self, written: [ssd: {}M]", writtenSize);
                System.exit(-1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    public DefaultMessageQueueImpl() {
        log.info("DefaultMessageQueueImpl 开始执行构造函数");
        DISC_ROOT = System.getProperty("os.name").contains("Windows") ? new File("d:/essd") : new File("/essd");
        PMEM_ROOT = System.getProperty("os.name").contains("Windows") ? new File("./pmem") : new File("/pmem");

        init();

        powerFailureRecovery(metaInfo);
        log.info("DefaultMessageQueueImpl 构造函数执行完成");
        killSelf(KILL_SELF_TIMEOUT);
    }

    public void powerFailureRecovery(ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo) {
        if (getTotalFileSize() == 0){
            return;
        }
        // 分组文件以及私有文件
        for (int id = 0; id < dataWriteChannels.length; id++) {
            ByteBuffer readBuffer = ByteBuffer.allocateDirect(DATA_INFORMATION_LENGTH);
            long baseAddress = ((DirectBuffer) readBuffer).address();
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
                    topicId = unsafe.getByte(baseAddress);
                    queueId = unsafe.getShort(baseAddress + 1);
                    dataLen = unsafe.getShort(baseAddress + 3);
                    offset = unsafe.getInt(baseAddress + 5);
                    topicInfo = metaInfo.computeIfAbsent(topicId, k -> new ConcurrentHashMap<>(2000));
                    queueInfo = topicInfo.computeIfAbsent(queueId, k -> new QueueInfo());
                    long groupIdAndDataLength = (((long) id) << 32) | dataLen;
                    if(topicId == (byte) 34 && queueId == 1 && offset == 26){
                        int a = 3;
                    }
                    queueInfo.setDataPosInFile(offset, channel.position(), groupIdAndDataLength);
                }
            } catch (Exception e) {
                System.out.println("ignore exception while rectory");
            }
        }
    }

    private ThreadWorkContent getWorkContent(){
        ThreadWorkContent threadWorkContent = threadWorkContentMap.get();
        if (threadWorkContent == null){
            int threadId = totalThreadCount.getAndIncrement();
            int groupId = threadId % groupCount;
            // 线程私有的channel，用来写单条数据到ssd
            int fileId = threadId + groupCount * 2;
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

        wrappedData.setWrapInfo(topicId, (short) queueId, data, offset, queueInfo);
        ramDataWriter.pushWrappedData(wrappedData);
//        pmemDataWriter.pushWrappedData(wrappedData);

        try {
            if (! cyclicBarriers[groupId].isBroken()){
                appendSsdByGroup(groupId, topicId, queueId, offset, queueInfo, data, dataLen, dataPosition);
                wrappedData.getMeta().getCountDownLatch().await();
            }else {
                // 单条写入
//                log.info("write single data");
                wrappedData.getMeta().getCountDownLatch().await();
                appendSsdBySelf(workContent, topicId, queueId, offset, queueInfo, data, dataLen, dataPosition);
            }
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
            if(groupWaitThreadCount[groupId].incrementAndGet() == awaitThreadCountLimits[groupId]){
                groupBuffers[groupId].position(0);
                groupBuffers[groupId].limit(groupBufferWritePos[groupId].get());
                dataWriteChannels[groupId].write(groupBuffers[groupId]);
                dataWriteChannels[groupId].force(true);
                groupBufferWritePos[groupId].set(0);
                groupWaitThreadCount[groupId].set(0);
            }
            cyclicBarriers[groupId].await(5, TimeUnit.SECONDS);
        } catch ( IOException | InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e){
//            log.info("BrokenBarrier one time");
        } catch (TimeoutException e) {
//            log.info("cyclicBarrier timeout.");
            // 这里把剩余的数据刷盘, WritePos 未归零时代表未刷盘
            if (groupBufferWritePos[groupId].get() != 0){
                synchronized (groupBuffers[groupId]){
                    if (groupBufferWritePos[groupId].get() != 0){
                        groupBuffers[groupId].position(0);
                        groupBuffers[groupId].limit(groupBufferWritePos[groupId].get());
                        try {
                            dataWriteChannels[groupId].write(groupBuffers[groupId]);
                            dataWriteChannels[groupId].force(true);
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                        groupBufferWritePos[groupId].set(0);
                        groupWaitThreadCount[groupId].set(0);
                    }
                }
            }
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



    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        GetRangeTaskData task = getTask(Thread.currentThread());

        task.setGetRangeParameter(topic, queueId, offset, fetchNum);
        task.queryData();
        return task.getResult();
    }


    public static final int threadCount = 40;
    public static final int topicCountPerThread = 2;  // threadCount * topicCountPerThread <= 100
    public static final int queueIdCountPerTopic = 5 * 2;
    public static final int writeTimesPerQueueId = 3 * 100;

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
        ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo = mq.metaInfo;
        for (Byte topicId : metaInfo.keySet()) {
            ConcurrentHashMap<Short, QueueInfo> topicInfo = metaInfo.get(topicId);
            for (Short queueId : topicInfo.keySet()) {
                QueueInfo queueInfo = topicInfo.get(queueId);
                for (int offset = 0; offset < queueInfo.size(); offset++) {
                    long[] p = queueInfo.getDataPosInFile(offset);
                    short dataLen = (short) p[1];
                    if (dataLen != 8){
                        int a = 1;
                    }
                }
            }
        }
    }
}

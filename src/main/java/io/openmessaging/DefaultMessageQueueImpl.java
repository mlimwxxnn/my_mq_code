package io.openmessaging;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class DefaultMessageQueueImpl extends MessageQueue {

    private static final Logger log = LoggerFactory.getLogger("myLogger");
    public static File DISC_ROOT;
    public static File PMEM_ROOT;
    public static final int DATA_INFORMATION_LENGTH = 9;
    public static final long KILL_SELF_TIMEOUT = 20 * 60;  // seconds
    public static final long WAITE_DATA_TIMEOUT = 1000;  // 微秒
    public static final int WRITE_THREAD_COUNT = 5;
    public static final int READ_THREAD_COUNT = 20;
    public static final int PMEM_PAGE_SIZE = 2 * 1024;
    public static final int PMEM_BLOCK_COUNT = 4;

    public static AtomicInteger topicCount = new AtomicInteger();
    static ConcurrentHashMap<String, Byte> topicNameToTopicId = new ConcurrentHashMap<>();
//    public static volatile ConcurrentHashMap<Byte, HashMap<Short, HashMap<Integer, long[]>>> metaInfo;
    public static volatile ConcurrentHashMap<Byte, HashMap<Short, QueueInfo>> metaInfo;
    public static volatile Map<Thread, GetRangeTask> getRangeTaskMap = new ConcurrentHashMap<>();
    public static final FileChannel[] dataWriteChannels = new FileChannel[WRITE_THREAD_COUNT];
    public static DataWriter dataWriter;
    public static DataReader dataReader;
    public static int initThreadCount = 0;
    public static PmemDataWriter pmemDataWriter;


    public static void init() {
        try {
            metaInfo = new ConcurrentHashMap<>(100);
            dataReader = new DataReader();
            for (int i = 0; i < WRITE_THREAD_COUNT; i++) {
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
            dataWriter = new DataWriter();
            pmemDataWriter = new PmemDataWriter();
            init_pmem();
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

    public static GetRangeTask getTask(Thread thread) {
        GetRangeTask task = getRangeTaskMap.get(thread);
        if (task != null) {
            return task;
        }
        task = new GetRangeTask();
        getRangeTaskMap.put(thread, task);
        return task;
    }

    public static void killSelf(long timeout) {
        if (timeout <= 0) {
            return;
        }
        new Thread(() -> {
            try {
                Thread.sleep(timeout * 1000);
                long writtenSize = 0;
                for (FileChannel dataWriteChannel : dataWriteChannels) {
                    writtenSize += dataWriteChannel.position();  // M
                }
                writtenSize /= (1024 * 1024);
                System.out.println(String.format("kill self(written: %d)", writtenSize));
                System.exit(-1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }


    private static void init_pmem(){
        boolean initialized = Heap.exists(PMEM_ROOT + "/persistent_heap");
        Heap h = initialized ? Heap.openHeap(PMEM_ROOT + "/persistent_heap") : Heap.createHeap(PMEM_ROOT + "/persistent_heap", 60*1024*1024*1024L);
        MemoryBlock newBlock;
        for (int i = 0; i < PMEM_BLOCK_COUNT; i++) {  // 创建pmem存储块
            newBlock = h.allocateMemoryBlock(57*1024*1024*1024L / PMEM_BLOCK_COUNT, false);  // todo 就当总内存是57G吧
            pmemDataWriter.memoryBlocks[i] = newBlock;
            for (int j = 0; j < 57*1024*1024*1024L / PMEM_BLOCK_COUNT / PMEM_PAGE_SIZE; j++) {
                pmemDataWriter.offerFreePage(new PmemPageInfo((byte)i, j)); // 对创建的内存块进行划分
            }
        }
    }

    public DefaultMessageQueueImpl() {
        log.info("DefaultMessageQueueImpl 开始执行构造函数");
        DISC_ROOT = System.getProperty("os.name").contains("Windows") ? new File("./essd") : new File("/essd");
        PMEM_ROOT = System.getProperty("os.name").contains("Windows") ? new File("./pmem") : new File("/pmem");
        init();
        killSelf(KILL_SELF_TIMEOUT);


//        logThreadCount();
        // 阻止数据恢复，不知道为什么不起作用
        if(getTotalFileSize()>0){
            System.exit(-1);
        }
        powerFailureRecovery(metaInfo);
        initThreadCount = Thread.activeCount();
        log.info("DefaultMessageQueueImpl 构造函数执行完成");
    }

    public void powerFailureRecovery(ConcurrentHashMap<Byte, HashMap<Short, QueueInfo>> metaInfo) {
        for (int id = 0; id < dataWriteChannels.length; id++) {
            FileChannel channel = dataWriteChannels[id];
            ByteBuffer readBuffer = ByteBuffer.allocate(DATA_INFORMATION_LENGTH);
            byte topicId;
            short queueId;
            short dataLen = 0;
            int offset;
            try {
                HashMap<Short, QueueInfo> topicInfo;
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

                    topicInfo = metaInfo.computeIfAbsent(topicId, k -> new HashMap<>(2000));
                    queueInfo = topicInfo.computeIfAbsent(queueId, k -> new QueueInfo());
                    long groupIdAndDataLength = (((long) id) << 32) | dataLen;
                    queueInfo.setDataPosInFile(offset, channel.position(), groupIdAndDataLength);
                }
            } catch (Exception e) {
                System.out.println("ignore exception while rectory");
            }
        }
    }


    @Override
    public long append(String topic, int queueId, ByteBuffer data) {

        haveAppended = true;
        System.out.println(data.remaining());
        Byte topicId = getTopicId(topic, true);

        HashMap<Short, QueueInfo> topicInfo = metaInfo.computeIfAbsent(topicId, k -> new HashMap<>(2000));
        QueueInfo queueInfo = topicInfo.computeIfAbsent((short) queueId, k -> new QueueInfo());
        int offset = queueInfo.size();

        WrappedData wrappedData = new WrappedData(topicId, (short) queueId, data, offset, queueInfo);
        dataWriter.pushWrappedData(wrappedData);
        pmemDataWriter.pushWrappedData(wrappedData);
        try {
            wrappedData.getMeta().getCountDownLatch().await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return offset;
    }

    /**
     * 创建或并返回topic对应的topicId
     */
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
    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {

        if(!haveAppended){
            System.exit(-1);
        }


        GetRangeTask task = getTask(Thread.currentThread());
        task.setGetRangeParameter(topic, queueId, offset, fetchNum);
        dataReader.pushTask(task);
        try {
            task.getCountDownLatch().await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return task.getResult();
    }

    public static void main(String[] args) throws InterruptedException {
        final int threadCount = 5;
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

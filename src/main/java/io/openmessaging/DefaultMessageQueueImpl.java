package io.openmessaging;

import javax.sound.midi.Soundbank;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class DefaultMessageQueueImpl extends MessageQueue {

    public static File DISC_ROOT;
    public static File PMEM_ROOT;
    public static final boolean isTestPowerFailure = true;
    public static final int DATA_INFORMATION_LENGTH = 7;
    public static final long KILL_SELF_TIMEOUT = 3 * 60;  // seconds
    public static final long WAITE_DATA_TIMEOUT = 500;  // 微秒
    public static final int WRITE_THREAD_COUNT = 5;

    public static AtomicInteger topicCount = new AtomicInteger();
    ConcurrentHashMap<String, Byte> topicNameToTopicId = new ConcurrentHashMap<>();
    public static volatile ConcurrentHashMap<Byte, ConcurrentHashMap<Integer, ArrayList<long[]>>> metaInfo;
    public static volatile Map<Thread, ThreadWorkContext> threadWorkContextMap = new ConcurrentHashMap<>();
    public static final FileChannel[] dataWriteChannels = new FileChannel[WRITE_THREAD_COUNT];
    public static final FileChannel[] dataReadChannels = new FileChannel[WRITE_THREAD_COUNT];
    public static DataWriter dataWriter;
    public static int initThreadCount = 0;
    public static OfficialDemo officialDemo = new OfficialDemo();

    static AtomicBoolean isBlockAppend = new AtomicBoolean();

    public static void init() throws IOException {
        metaInfo = new ConcurrentHashMap<>();
        for (int i = 0; i < WRITE_THREAD_COUNT; i++) {
            File file = new File(DISC_ROOT, "data-" + i);
            File parentFile = file.getParentFile();
            if (!parentFile.exists()){
                parentFile.mkdirs();
            }
            if (!file.exists()){
                file.createNewFile();
            }
            dataWriteChannels[i] = FileChannel.open(file.toPath(), StandardOpenOption.APPEND, StandardOpenOption.WRITE);
            dataReadChannels[i] = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        }
        dataWriter = new DataWriter();
    }

    public static long getTotalFileSize(){
        long fileSize = 0;
        for (FileChannel dataWriteChannel : dataReadChannels) {
            try {
                fileSize += dataWriteChannel.size();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileSize;
    }
    static class ThreadWorkContext {
        public final ByteBuffer[] buffers = new ByteBuffer[100]; // 用来响应查询的buffer
        public ThreadWorkContext(){
            for (int i = 0; i < buffers.length; i++) {
                buffers[i] = ByteBuffer.allocateDirect(17 * 1024);
            }
        }
    }

    public static ThreadWorkContext getThreadWorkContext(Thread thread){
        ThreadWorkContext context = threadWorkContextMap.get(thread);
        if (context != null){
            return context;
        }
        context = new ThreadWorkContext();
        threadWorkContextMap.put(thread, context);
        return context;
    }

    public static void killSelf(long timeout) {
        new Thread(()->{
            try {
                Thread.sleep(timeout * 1000);
                long writtenSize = 0;
                for (FileChannel dataWriteChannel : dataWriteChannels) {
                    writtenSize += dataWriteChannel.position() / (1024 * 1024);  // M
                }
                System.out.println(String.format("kill self(written: %d)", writtenSize));
                System.exit(-1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    public DefaultMessageQueueImpl() {
        DISC_ROOT = System.getProperty("os.name").contains("Windows") ? new File("./essd") : new File("/essd");
        PMEM_ROOT = System.getProperty("os.name").contains("Windows") ? new File("./pmem") : new File("/pmem");
        try {
            init();
        } catch (IOException e) {
            System.out.println("init failed");
        }

        killSelf(KILL_SELF_TIMEOUT);

        // 如果数据文件里有东西就从文件恢复索引
        powerFailureRecovery(metaInfo);
        initThreadCount = Thread.activeCount();

    }

    public static void powerFailureRecovery(ConcurrentHashMap<Byte, ConcurrentHashMap<Integer, ArrayList<long[]>>> metaInfo) {
        if (getTotalFileSize() > 0) {
            try {
                for (int id = 0; id < dataReadChannels.length; id++) {
                    FileChannel channel = dataReadChannels[id];
                    ByteBuffer readBuffer = ByteBuffer.allocate(DATA_INFORMATION_LENGTH);
                    byte topicId;
                    int queueId;
                    short dataLen = 0;
                    ConcurrentHashMap<Integer, ArrayList<long[]>> topicInfo;
                    ArrayList<long[]> queueInfo;
                    long dataFilesize = channel.size();
                    while (channel.position() + dataLen < dataFilesize) {
                        channel.position(channel.position() + dataLen); // 跳过数据部分只读取数据头部的索引信息
                        readBuffer.clear();
                        channel.read(readBuffer);

                        readBuffer.flip();
                        topicId = readBuffer.get();
                        queueId = readBuffer.getInt();
                        dataLen = readBuffer.getShort();
                        topicInfo = metaInfo.get(topicId);
                        if (topicInfo == null) {
                            topicInfo = new ConcurrentHashMap<>(5000);
                            metaInfo.put(topicId, topicInfo);
                        }
                        queueInfo = topicInfo.get(queueId);
                        if (queueInfo == null) {
                            queueInfo = new ArrayList<>(64);
                            topicInfo.put(queueId, queueInfo);
                        }
                        long groupIdAndDataLength = (((long) id) << 32) | dataLen;
                        queueInfo.add(new long[]{channel.position(), groupIdAndDataLength});
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            if(isTestPowerFailure){
                testPowerFailureRecovery();
            }
        }
    }

    public static void testPowerFailureRecovery(){
        new Thread(()->{
            try {
                Thread.sleep(10 * 1000);
                isBlockAppend.set(true);
                Thread.sleep(3 * 1000);
                DefaultMessageQueueImpl myDemo = new DefaultMessageQueueImpl();
                if(isMyDemoRight(officialDemo, myDemo)){
                    System.out.println("my demo is right for power failure recovery!");
                }else{
                    System.out.println("not right!");
                }
                System.exit(-1);

            }catch (Exception ex){
                ex.printStackTrace();
                System.exit(-1);
            }
        }).start();
    }

    private static boolean isMyDemoRight(OfficialDemo officialDemo, DefaultMessageQueueImpl myDemo) {
        ConcurrentHashMap<String, Map<Integer, Long>> appendOffset = officialDemo.appendOffset;
        ConcurrentHashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = officialDemo.appendData;
        for (String topic : appendOffset.keySet()) {
            Map<Integer, Long> queueIdLenMap = appendOffset.get(topic);
            for (Integer queueId : queueIdLenMap.keySet()) {
                Long offsetUpToNow = queueIdLenMap.get(queueId);
                int fetchNum = 20;
                for (long offset = 0L; offset + fetchNum < offsetUpToNow; offset+=fetchNum) {
                    Map<Integer, ByteBuffer> officialRes = officialDemo.getRange(topic, queueId, offset, fetchNum);
                    Map<Integer, ByteBuffer> myRes = myDemo.getRange(topic, queueId, offset, fetchNum);
                    if (!officialRes.equals(myRes)){
                        System.out.println(String.format("query topic: %s, queueId: %d, offset: %d, fetchNum: %d", topic, queueId, offset, fetchNum));
                        System.out.printf("official: (%d)\n", officialRes.size());
                        System.out.println(officialRes);
                        System.out.printf("mine: (%d)\n", myRes.size());
                        System.out.println(myRes);
                        return false;
                    }
                }
            }
        }
        return true;
    }


    //    public volatile Map<Thread, Integer> appendThread = new ConcurrentHashMap<>();
    public static volatile Map<String, String> appendDone = new ConcurrentHashMap<>();
    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
//        Thread thread = Thread.currentThread();
//        appendThread.put(Thread.currentThread(), appendThread.getOrDefault(Thread.currentThread(), 0) + 1);

//        while(isBlockAppend.get()){
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
        if (isTestPowerFailure){
            if (isBlockAppend.get()){
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            officialDemo.append(topic, queueId, data);
        }

        Byte topicId = getTopicId(topic, true);
//        String key = topicId + "-" + queueId;
//        appendDone.put(key, key);

        WrappedData wrappedData = new WrappedData(topicId, queueId, data);
        dataWriter.pushWrappedData(wrappedData);
        try {
            wrappedData.getMeta().countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return metaInfo.get(topicId).get(queueId).size() - 1;
    }

    /**
     * 创建或并返回topic对应的topicId
     *
     * @param topic 话题名
     * @return
     * @throws IOException
     */
    private Byte getTopicId(String topic, boolean isCreateNew) {
        Byte topicId = topicNameToTopicId.get(topic);
        if (topicId == null) {
            synchronized (topicNameToTopicId) {
                if((topicId = topicNameToTopicId.get(topic)) == null) {
                    File topicIdFile = new File(DISC_ROOT, topic);
                    try {
                        if (!topicIdFile.exists()) {
                            if (!isCreateNew){
                                return null;
                            }
                            // 文件不存在，这是一个新的Topic，保存topic名称到topicId的映射，文件名为topic，内容为id
                            topicNameToTopicId.put(topic, topicId = (byte) topicCount.getAndIncrement());
                            FileOutputStream fos = new FileOutputStream(new File(DISC_ROOT, topic));
                            fos.write(topicId);
                            fos.flush();
                            fos.close();
                        } else {
                            // 文件存在，topic不在内存，从文件恢复
                            FileInputStream fis = new FileInputStream(new File(DISC_ROOT, topic));
                            topicId = (byte) fis.read();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return topicId;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        try {
            HashMap<Integer, ByteBuffer> ret = new HashMap<>();

            Byte topicId = getTopicId(topic, false);
            if (topicId == null){
                return ret;
            }

            ArrayList<long[]> queueInfo = metaInfo.get(topicId).get(queueId);
            if(queueInfo == null){
                return ret;
//                String key = topicId + "-" + queueId;
//                System.out.println("是否append过这个数据：" + appendDone.containsKey(key));
//                System.out.println("是否write过这个数据：" + dataWriter.done.containsKey(key));
//                System.out.println("queueInfo 为空");
//                System.exit(-1);
            }
            ThreadWorkContext context = getThreadWorkContext(Thread.currentThread());
            ByteBuffer[] buffers = context.buffers;
            try {
                for (int i = 0; i < fetchNum && (i + offset) < queueInfo.size(); i++) {
                    long[] p = queueInfo.get(i + (int) offset);
                    ByteBuffer buf = buffers[i];
                    buf.clear();
                    buf.limit((int) p[1]);
                    int id = (int) (p[1] >> 32);
                    dataReadChannels[id].read(buf, p[0]);
                    buf.flip();
                    ret.put(i, buf);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            return ret;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws InterruptedException {
        final int threadCount = 40;
        final int topicCountPerThread = 2;  // threadCount * topicCountPerThread <= 100
        final int queueIdCountPerTopic = 200;
        final int writeTimesPerQueueId = 60;
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
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int topicIndex = 0; topicIndex < topicCountPerThread; topicIndex++) {
                        String topic = threadIndex + "-" + topicIndex;
                        for (int queueIndex = 0; queueIndex < queueIdCountPerTopic; queueIndex++) {
                            for (int t = 0; t < writeTimesPerQueueId; t++) {
                                mq.append(topic, queueIndex, buffers[threadIndex][topicIndex][queueIndex]);
                            }
                            mq.getRange(topic, queueIndex, 0, 100);
                        }
                    }
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }

        Map<Integer, ByteBuffer> res = mq.getRange("10-1", 15, 0, 100);
        System.out.println(res.size());
    }
}

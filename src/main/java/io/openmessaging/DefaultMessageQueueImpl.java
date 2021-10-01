package io.openmessaging;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class DefaultMessageQueueImpl extends MessageQueue {

    public static final boolean DEBUG = false;
    public static File DISC_ROOT;
    public static File PMEM_ROOT;
    public static final AtomicInteger appendCount = new AtomicInteger();
    public static final AtomicInteger getRangeCount = new AtomicInteger();
    public static final long KILL_SELF_TIMEOUT = 869;  // seconds
    public static final long THREAD_PARK_TIMEOUT = 2;  // ms
    public static AtomicInteger MERGE_MIN_THREAD_COUNT = new AtomicInteger(5);  // 只是起始
    public static final int groupCount = 5;
    public static final int READ_SEMAPHORE_PER_GROUP = 2;

    public static AtomicInteger topicCount = new AtomicInteger();
    ConcurrentHashMap<String, Byte> topicNameToTopicId = new ConcurrentHashMap<>();
    // topicId, queueId, dataPosition
    public static volatile ConcurrentHashMap<Byte, ConcurrentHashMap<Integer, ArrayList<Long>>> metaInfo = new ConcurrentHashMap<>();
    public static final Unsafe unsafe = UnsafeUtil.unsafe;


    public static final int DATA_INFORMATION_LENGTH = 7;

    public static volatile Map<Thread, Integer> groupIdMap = new ConcurrentHashMap<>();
    public static volatile Map<Thread, ThreadWorkContext> threadWorkContextMap = new ConcurrentHashMap<>();

    public static final AtomicInteger threadCountNow = new AtomicInteger();
    public static final FileChannel[] dataWriteChannels = new FileChannel[groupCount];
    public static final FileChannel[] dataReadChannels = new FileChannel[groupCount];
    public static volatile ByteBuffer[] mergeBuffers = new ByteBuffer[groupCount];
//    public static final ReentrantLock[] mergeBufferLocks = new ReentrantLock[groupCount];
    public static final ReentrantReadWriteLock[] mergeBufferRWLocks = new ReentrantReadWriteLock[groupCount];
    public static final AtomicLong[] mergeBufferPositions = new AtomicLong[groupCount];
    public static volatile Map<Thread, Thread>[] parkedThreadMaps = new Map[groupCount];
    public static final AtomicLong[] forceVersions = new AtomicLong[groupCount];
    public static final Semaphore[] readSemaphores = new Semaphore[groupCount];
    public static int maxThreadCountPerGroup = (50 + groupCount - 1) / groupCount;

    public static void init() throws IOException {
        for (int i = 0; i < groupCount; i++) {
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
            mergeBuffers[i] = ByteBuffer.allocateDirect(18 * 1024 * maxThreadCountPerGroup);
            mergeBufferRWLocks[i] = new ReentrantReadWriteLock();
            mergeBufferPositions[i] = new AtomicLong(((DirectBuffer) mergeBuffers[i]).address());
            parkedThreadMaps[i] = new ConcurrentHashMap<>();
            forceVersions[i] = new AtomicLong();
            readSemaphores[i] = new Semaphore(READ_SEMAPHORE_PER_GROUP);
        }
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

    public static void killSelf(long timeout) {
        new Thread(()->{
            try {
                Thread.sleep(timeout * 1000);
                long writtenSize = 0;
                for (FileChannel dataWriteChannel : dataWriteChannels) {
                    writtenSize += dataWriteChannel.position() / (1024 * 1024);  // M
                }
                System.out.println(String.format("kill myself(written: %d)", writtenSize));
                System.exit(-1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    static class ThreadWorkContext {
        public final int id; // groupId
        public final ByteBuffer[] buffers = new ByteBuffer[100]; // 用来响应查询的buffer
        public ThreadWorkContext(int id){
            this.id = id;
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
        int id = threadCountNow.getAndIncrement() % groupCount;
        context = new ThreadWorkContext(id);

        // threadCountNow.get() / groupCount - 5 为每个分组的线程数少 5 个为最小 merge 数
        MERGE_MIN_THREAD_COUNT.set(Math.max(threadCountNow.get() / groupCount - 3, MERGE_MIN_THREAD_COUNT.get()));
        threadWorkContextMap.put(thread, context);
        return context;
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
        if (getTotalFileSize() > 0) {
            try {
                for (int id = 0; id < dataReadChannels.length; id++) {
                    FileChannel channel = dataReadChannels[id];
                    ByteBuffer readBuffer = ByteBuffer.allocate(DATA_INFORMATION_LENGTH);
                    byte topicId;
                    int queueId;
                    short dataLen = 0;
                    ConcurrentHashMap<Integer, ArrayList<Long>> topicInfo;
                    ArrayList<Long> queueInfo;
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
                        long pos = channel.position();
                        pos |= ((long)dataLen) << 40;
                        pos |= ((long)id) << 56;
                        queueInfo.add(pos);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        // 打印所有已经append的条数
        if (DEBUG){
            int appendCountNow = appendCount.incrementAndGet();
            if (appendCountNow % 10000 == 0){
                System.out.println(String.format("appendCountNow: %d", appendCountNow));
            }
        }
        try {
            Thread selfThread = Thread.currentThread();
            ThreadWorkContext context = getThreadWorkContext(selfThread);
            int id = context.id;

            FileChannel dataWriteChannel = dataWriteChannels[id];
            ByteBuffer mergeBuffer = mergeBuffers[id];
//            ReentrantLock mergeBufferLock = mergeBufferLocks[id];
            ReentrantReadWriteLock mergeBufferRWLock = mergeBufferRWLocks[id];
            AtomicLong mergeBufferPosition = mergeBufferPositions[id];
            Map<Thread, Thread> parkedThreadMap = parkedThreadMaps[id];
            AtomicLong forceVersion = forceVersions[id];

            long offset;
            byte topicId = getTopicId(topic);
            int dataLength = data.remaining(); // 默认data的position是从 0 开始的

            // 根据topicId获取topic下的全部队列的信息
            ConcurrentHashMap<Integer, ArrayList<Long>> topicInfo = metaInfo.get(topicId);
            if (topicInfo == null) {
                topicInfo = new ConcurrentHashMap<>();
                metaInfo.put(topicId, topicInfo);
            }
            // 根据queueId获取队列在文件中的位置信息
            ArrayList<Long> queueInfo = topicInfo.get(queueId);
            if (queueInfo == null) {
                queueInfo = new ArrayList<>();
                topicInfo.put(queueId, queueInfo);
            }
            offset = queueInfo.size();
            long initialAddress = ((DirectBuffer) mergeBuffer).address();

            mergeBufferRWLock.readLock().lock();
            long channelPosition = dataWriteChannel.position();
            long writeAddress = mergeBufferPosition.getAndAdd(DATA_INFORMATION_LENGTH + dataLength);
            int index = (int)(writeAddress - initialAddress);
            mergeBuffer.put(index, topicId);
            mergeBuffer.putInt(index + 1, queueId);
            mergeBuffer.putShort(index + 5, (short) dataLength);
            unsafe.copyMemory(data.array(), 16 + data.position(), null, writeAddress + DATA_INFORMATION_LENGTH, dataLength);

            // 登记需要刷盘的数据和对应的线程
            long forceVersionNow = forceVersion.get();
            parkedThreadMap.put(selfThread, selfThread);
            // 在这里读阻塞数，避免判断阻塞数量时和后续的 clear 操作冲突
            int parkedThreadCount = parkedThreadMap.size();
            mergeBufferRWLock.readLock().unlock();

            if(parkedThreadCount < MERGE_MIN_THREAD_COUNT.get()) {
                long start = System.currentTimeMillis();
                unsafe.park(true, System.currentTimeMillis() + THREAD_PARK_TIMEOUT);  // ms
                long stop = System.currentTimeMillis();
                if (DEBUG){
                    System.out.println(String.format("Thread: %s, id: %d, park for time : %d ms", selfThread.getName(), id, stop - start));
                }
            }
            // 自己的 data 还没被 force
            if (forceVersionNow == forceVersion.get()){
                mergeBufferRWLock.writeLock().lock();
                if (forceVersionNow == forceVersion.get()){
                    long start = System.currentTimeMillis();
                    mergeBuffer.limit((int) (mergeBufferPosition.get() - initialAddress));
                    dataWriteChannel.write(mergeBuffer);
                    dataWriteChannel.force(true);
                    long stop = System.currentTimeMillis();

                    if (DEBUG){
                        System.out.println(String.format("groupId: %d, mergeThreadCount: %d, mergeSize: %d kb, writeCostTime: %d", id, parkedThreadMap.size(), mergeBuffer.limit() / 1024, stop - start));
                    }

                    mergeBuffer.clear();
                    mergeBufferPosition.set(initialAddress);
                    forceVersion.getAndIncrement();
                    // 叫醒各个线程
                    parkedThreadMap.remove(selfThread);
                    for (Thread thread : parkedThreadMap.keySet()) {
                        unsafe.unpark(thread);
                    }
                    parkedThreadMap.clear();
                }
                mergeBufferRWLock.writeLock().unlock();
            }
            long pos = channelPosition + writeAddress - initialAddress + DATA_INFORMATION_LENGTH;
            pos |= ((long)dataLength) << 40;
            pos |= ((long)id) << 56;
            queueInfo.add(pos);
            return offset;
        } catch (Exception ignored) { }
        return 0L;
    }

    /**
     * 创建或并返回topic对应的topicId
     *
     * @param topic 话题名
     * @return
     * @throws IOException
     */
    private Byte getTopicId(String topic) {
        Byte topicId = topicNameToTopicId.get(topic);
        if (topicId == null) {
            synchronized (topicNameToTopicId) {
                if((topicId = topicNameToTopicId.get(topic)) == null) {
                    File topicIdFile = new File(DISC_ROOT, topic);
                    try {
                        if (!topicIdFile.exists()) {
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
                    } catch (Exception ignored) {
                    }
                }
            }
        }
        return topicId;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Byte topicId = getTopicId(topic);
        ArrayList<Long> queueInfo = metaInfo.get(topicId).get(queueId);

        HashMap<Integer, ByteBuffer> ret = new HashMap<>();
        ThreadWorkContext context = getThreadWorkContext(Thread.currentThread());
        ByteBuffer[] buffers = context.buffers;
        boolean acquiredReadSemaphore = false;
        int groupId = 0;
        try {
            for (int i = 0; i < fetchNum && (i + offset) < queueInfo.size(); i++){
                long p = queueInfo.get(i + (int) offset);
                long dataPosition = p & 0xffffffffffL;
                int dataLen = (int)((p>>>40) & 0xffff);
                int id = (int)(p >>> 56);
                if (!acquiredReadSemaphore){
                    readSemaphores[id].acquire();
                    groupId = id;
                    acquiredReadSemaphore = true;
                }
                ByteBuffer buf = buffers[i];
                buf.clear();
                buf.limit(dataLen);
                dataReadChannels[id].read(buf, dataPosition);
                buf.flip();
                ret.put(i, buf);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (acquiredReadSemaphore){
                readSemaphores[groupId].release();
            }
        }
        // 打印总体已经响应查询的次数
        if (DEBUG){
            int getRangeCountNow = getRangeCount.incrementAndGet();
            if (getRangeCountNow % 10000 == 0){
                System.out.println("getRangeCountNow: " + getRangeCountNow);
            }
        }
        return ret;
    }

    public static void main(String[] args) throws InterruptedException {
        final int threadCount = 25;
        final int topicCountPerThread = 4;  // threadCount * topicCountPerThread <= 100
        final int queueIdCountPerTopic = 20;
        final int writeTimesPerQueueId = 30;
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
                        }
                    }
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }

        Map<Integer, ByteBuffer> res = mq.getRange("10-3", 15, 0, 100);
        System.out.println(res.size());
    }
}

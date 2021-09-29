package io.openmessaging;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class DefaultMessageQueueImpl extends MessageQueue {

    public static final boolean DEBUG = true;
    public static final File DISC_ROOT = new File("/essd");
    public static final File PMEM_ROOT = new File("/pmem");
    public static final File dataFile = new File(DISC_ROOT, "data");
    public static FileChannel dataWriteChannel;
    public static FileChannel dataReadChannel;
    public static final Object lockObj = new Object();
    public static final AtomicInteger appendCount = new AtomicInteger();
    public static final AtomicInteger getRangeCount = new AtomicInteger();
    public static final int THREAD_PARK_TIMEOUT = 10;
    public static final int MERGE_MIN_THREAD_COUNT = 20;
    static {
        try {
            if (!dataFile.exists()) dataFile.createNewFile();
            dataWriteChannel = FileChannel.open(dataFile.toPath(),
                    StandardOpenOption.APPEND, StandardOpenOption.WRITE);
            dataReadChannel = FileChannel.open(dataFile.toPath(), StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static AtomicInteger topicCount = new AtomicInteger();
    ConcurrentHashMap<String, Byte> topicNameToTopicId = new ConcurrentHashMap<>();
    // topicId, queueId, dataPosition
    public static volatile ConcurrentHashMap<Byte, ConcurrentHashMap<Integer, ArrayList<long[]>>> metaInfo = new ConcurrentHashMap<>();
    public static final Unsafe unsafe = UnsafeUtil.unsafe;
    // 用来控制不同轮次的数据不被干扰
    public static final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    public static final Lock writeLock = readWriteLock.writeLock();
    public static final Lock readLock = readWriteLock.readLock();
    public static volatile Map<Thread, Thread> parkedThreadSet = new ConcurrentHashMap<>(100);  // 存储调用过append方法的线程 k: thread, v: self
    public static volatile Map<ByteBuffer, ByteBuffer> dataToForceSet = new ConcurrentHashMap<>();  // 存储需要被force的data, k: byteBuffer, v: self

    public static volatile ByteBuffer mergeBuffer = ByteBuffer.allocateDirect(18 * 1024 * 50);
    public static final AtomicLong mergeBufferPosition = new AtomicLong(((DirectBuffer) mergeBuffer).address());
    public static final AtomicBoolean isForcing = new AtomicBoolean(false);

    public static final long TIMEOUT = 3*60;  // seconds
    public static final int DATA_INFORMATION_LENGTH = 7;

    public static void killSelf(long timeout) {
        new Thread(()->{
            try {
                Thread.sleep(timeout * 1000);
                long writtenSize = dataWriteChannel.position() / (1024 * 1024);  // M
                System.out.println(String.format("kill myself(written: %d)", writtenSize));
                System.exit(-1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    public DefaultMessageQueueImpl() {
        killSelf(TIMEOUT);

        long dataFilesize;

        // 如果数据文件里有东西就从文件恢复索引
        if ((dataFilesize = dataFile.length()) > 0) {
            try {
                ByteBuffer readBuffer = ByteBuffer.allocate(DATA_INFORMATION_LENGTH);
                FileChannel channel = dataReadChannel;
                byte topicId;
                int queueId;
                short dataLen = 0;
                ConcurrentHashMap<Integer, ArrayList<long[]>> topicInfo;
                ArrayList<long[]> queueInfo;
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
                    queueInfo.add(new long[]{channel.position(), dataLen});
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
            try {
                int appendCountNow = appendCount.incrementAndGet();
                long writtenSize = dataWriteChannel.position() / (1024 * 1024);  // M
                if (appendCountNow % 10000 == 0){
                    System.out.println(String.format("appendCountNow: %d, writtenSize: %d", appendCountNow, writtenSize));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        try {
            long offset;
            byte topicId = getTopicId(topic);
            int dataLength = data.remaining(); // 默认data的position是从 0 开始的
            long channelPosition = dataWriteChannel.position();

            // 根据topicId获取topic下的全部队列的信息
            ConcurrentHashMap<Integer, ArrayList<long[]>> topicInfo = metaInfo.get(topicId);
            if (topicInfo == null) {
                topicInfo = new ConcurrentHashMap<>();
                metaInfo.put(topicId, topicInfo);
            }
            // 根据queueId获取队列在文件中的位置信息
            ArrayList<long[]> queueInfo = topicInfo.get(queueId);
            if (queueInfo == null) {
                queueInfo = new ArrayList<>();
                topicInfo.put(queueId, queueInfo);
            }
            offset = queueInfo.size();
            long initialAddress = ((DirectBuffer) mergeBuffer).address();

            long l1 = System.currentTimeMillis();
//            readLock.lock();
            long l2 = System.currentTimeMillis();
            // 如果正在刷盘中，就自旋等待
            while (isForcing.get()){
                Thread.sleep(1);
            };
            long writeAddress = mergeBufferPosition.getAndAdd(DATA_INFORMATION_LENGTH + dataLength);
            int index = (int)(writeAddress - initialAddress);
            mergeBuffer.put(index, topicId);
            mergeBuffer.putInt(index + 1, queueId);
            mergeBuffer.putShort(index + 5, (short) dataLength);
            unsafe.copyMemory(data.array(), 16 + data.position(), null, writeAddress + DATA_INFORMATION_LENGTH, dataLength);
            long l3 = System.currentTimeMillis();
//            readLock.unlock();
            long l4 = System.currentTimeMillis();
            dataToForceSet.put(data, data);
            parkedThreadSet.put(Thread.currentThread(), Thread.currentThread());
            if(parkedThreadSet.size() < MERGE_MIN_THREAD_COUNT) {
                // 登记需要刷盘的数据
                // 登记需要被唤醒的数据
                long l5 = System.currentTimeMillis();
                System.out.println("l4 - l5: " + (l5 - l4));
                unsafe.park(true, THREAD_PARK_TIMEOUT);
            }

            long l6 = System.currentTimeMillis();
            // 自己的 data 还没被 force
            if (dataToForceSet.containsKey(data)){
//                writeLock.lock();
                isForcing.set(true);

                long l7 = System.currentTimeMillis();

                // 等待各个线程拷贝完毕
                while (parkedThreadSet.size() != dataToForceSet.size()){
                    Thread.sleep(1);
                }
                dataToForceSet.clear();
                mergeBuffer.clear();



                mergeBuffer.limit((int) (mergeBufferPosition.get() - initialAddress));
                long start = System.currentTimeMillis();
                dataWriteChannel.write(mergeBuffer);
                dataWriteChannel.force(true);
                long stop = System.currentTimeMillis();
                System.out.println(String.format("mergeSize: %d kb, use time: %d", mergeBuffer.limit() / 1024, stop - start));
                // 叫醒各个线程
                for (Thread thread : parkedThreadSet.keySet()) {
                    unsafe.unpark(thread);
                }
                parkedThreadSet.clear();
                mergeBufferPosition.set(initialAddress);
                long l8 = System.currentTimeMillis();

                System.out.println(String.format("l1 - l4: %d, l2 - l3: %d, l6 - l7: %d, l7 - l8: %d", l4 - l1, l3 - l2, l7 - l6, l8 - l7));
//                writeLock.unlock();
                isForcing.set(false);
            }
            long pos = channelPosition + writeAddress - initialAddress + DATA_INFORMATION_LENGTH;
            queueInfo.add(new long[]{pos, dataLength}); // todo: 占用大小待优化
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
            }catch(Exception ignored) {}
        }
        return topicId;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Byte topicId = getTopicId(topic);
        ArrayList<long[]> queueInfo = metaInfo.get(topicId).get(queueId);
        // 这里改了
        // ByteBuffer readerBuffer = ByteBuffer.allocateDirect(17 * 1024);
        HashMap<Integer, ByteBuffer> ret = new HashMap<>();
        try {
            synchronized (queueInfo) {
                for (int i = (int) offset; i < (int) (offset + fetchNum) && i < queueInfo.size(); ++i) {
                    long[] p = queueInfo.get(i);
                    ByteBuffer buf = ByteBuffer.allocate((int) p[1]);
                    dataReadChannel.read(buf, p[0]);
                    buf.flip();
                    ret.put(i, buf);
                }
            }
        }catch (Exception ignored){ }
        // 打印总体已经响应查询的次数
        if (DEBUG){
            int getRangeCountNow = getRangeCount.incrementAndGet();
            if (getRangeCountNow % 10000 == 0){
                System.out.println("getRangeCountNow: " + getRangeCountNow);
            }
        }
        return ret;
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        DefaultMessageQueueImpl inst = new DefaultMessageQueueImpl();
        ByteBuffer[] buffers = new ByteBuffer[100];
        for (byte i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocate(4);
            buffers[i].put(i);
            buffers[i].flip();
        }

        long offset;
        offset = inst.append("abc", 1000, buffers[77]);
        offset = inst.append("abc", 1001, buffers[67]);
        offset = inst.append("abc", 1001, buffers[42]);
        offset = inst.append("abc", 1001, buffers[33]);
        offset = inst.append("abd", 1001, buffers[11]);

        Map<Integer, ByteBuffer> ret = inst.getRange("abc", 1001, 0, 100);
        System.out.println(String.format("Keys: %d", ret.keySet().size()));

    }
}

package io.openmessaging;

import sun.misc.Unsafe;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class DefaultMessageQueueImpl extends MessageQueue {

    public static final File DISC_ROOT = new File("./essd");
    public static final File PMEM_ROOT = new File("./pmem");
    public static final File dataFile = new File(DISC_ROOT, "data");
    public static FileChannel dataWriteChannel;
    public static FileChannel dataReadChannel;
    public static final Object lockObj = new Object();
    public static final AtomicInteger appendCount = new AtomicInteger();
    public static final AtomicInteger getRangeCount = new AtomicInteger();
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
    public ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
    // topicId, queueId, dataPosition
    public static ConcurrentHashMap<Byte, ConcurrentHashMap<Integer, ArrayList<long[]>>> metaInfo = new ConcurrentHashMap<>();
    public static final Unsafe unsafe = UnsafeUtil.unsafe;
    public static HashSet<Thread> threadSet = new HashSet<>(100);  // 存储调用过append方法的线程
    public static AtomicInteger blockedTheadCount = new AtomicInteger();
    public static long forcedDataPosition = 0;
    public static final long TIMEOUT = 5*60;  // seconds

    public static void killSelf(long timeout) {
        new Thread(()->{
            try {
                Thread.sleep(timeout * 1000);
                System.out.println("kill myself");
                System.exit(-1);
            } catch (InterruptedException e) {
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
                ByteBuffer readBuffer = ByteBuffer.allocate(7);
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
        int appendCountNow = appendCount.incrementAndGet();
        if (appendCountNow % 10000 == 0){
            System.out.println("appendCountNow: " + appendCountNow);
        }
        try {
            long offset;
            long pos;
            threadSet.add(Thread.currentThread());
            byte topicId = getTopicId(topic);

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

            ByteBuffer outBuffer = ByteBuffer.allocate(8);
            outBuffer.put(topicId);
            outBuffer.putInt(queueId);
            outBuffer.putShort((short)data.remaining());
            outBuffer.flip();


            synchronized (lockObj) {
                dataWriteChannel.write(outBuffer);
                dataWriteChannel.write(data);
                pos = dataWriteChannel.position();
            }

            if(blockedTheadCount.get() < 10) {
                blockedTheadCount.getAndIncrement();
                unsafe.park(true, 10);
            }
            if(pos > forcedDataPosition) {
                synchronized(lockObj){
                    if(pos > forcedDataPosition) {
                        dataWriteChannel.force(true);
                        // 这里改了
                        // forcedDataPosition = pos;
                        forcedDataPosition = dataWriteChannel.position();
                        threadSet.forEach(unsafe::unpark);
                    }
                }
            }

            queueInfo.add(new long[]{pos - data.limit(), data.limit()}); // todo: 占用大小待优化
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
        ByteBuffer readerBuffer = ByteBuffer.allocateDirect(17 * 1024);
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

        int getRangeCountNow = getRangeCount.incrementAndGet();
        if (getRangeCountNow % 10000 == 0){
            System.out.println("getRangeCountNow: " + getRangeCountNow);
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

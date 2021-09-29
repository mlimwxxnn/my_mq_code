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

    public static final File DISC_ROOT = new File("/essd");
    public static final File PMEM_ROOT = new File("/pmem");
    public static final File dataFile = new File(DISC_ROOT, "data");
    public static FileChannel dataChannel;

    static {
        try {
            dataChannel = FileChannel.open(new File(DISC_ROOT, "dataFile").toPath(),
                    StandardOpenOption.APPEND, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static AtomicInteger topicCount = new AtomicInteger();
    ConcurrentHashMap<String, Byte> topicNameToTopicId = new ConcurrentHashMap<>();
    public ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
    // topicId, queueId, dataPosition
    ConcurrentHashMap<Byte, ConcurrentHashMap<Integer, ArrayList<long[]>>> metaInfo = new ConcurrentHashMap<>();


    public DefaultMessageQueueImpl() {
        long dataFilesize;

        // 如果数据文件里有东西就从文件恢复索引
        if ((dataFilesize = dataFile.length()) > 0) {
            try {
                ByteBuffer readBuffer = ByteBuffer.allocate(8);
                FileChannel channel = FileChannel.open(dataFile.toPath(), StandardOpenOption.READ);
                channel.read(readBuffer);
                readBuffer.flip();
                byte topicId;
                int queueId;
                short dataLen = 0;
                ConcurrentHashMap<Integer, ArrayList<long[]>> topicInfo;
                ArrayList<long[]> queueInfo;
                while (channel.position() < dataFilesize) {
                    channel.position(channel.position() + dataLen); // 跳过数据部分只读取数据头部的索引信息
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
        long offset;
        try {
            File topicFolder = new File(DISC_ROOT, topic);
            byte topicId = getTopicId(topic);

            // 根据topicId获取topic下的队列信息
            ConcurrentHashMap<Integer, ArrayList<long[]>> topicInfo = metaInfo.get(topicId);
            if(topicInfo == null) {
                topicInfo = new ConcurrentHashMap<Integer, ArrayList<long[]>>();
                metaInfo.put(topicId, topicInfo);
            }
            // 根据queueId获取队列在文件中的位置信息
            ArrayList<long[]> queueInfo = topicInfo.get(queueId);
            if(queueInfo == null){
                queueInfo = new ArrayList<>();
                topicInfo.put(queueId, queueInfo);
            }
            offset = queueInfo.size();



            return offset;
        } catch (Exception e) {
            e.printStackTrace();
            return 0L;
        }
    }

    /**
     * 创建或并返回topic对应的topicId
     * @param topic 话题名
     * @return
     * @throws IOException
     */
    private Byte getTopicId(String topic) throws IOException {

        Byte topicId = topicNameToTopicId.get(topic);
        if (topicId == null) {
            File topicIdFile = new File(DISC_ROOT, topic);
            if (!topicIdFile.exists()) {
                // 文件不存在，这是一个新的Topic，保存topic名称到topicId的映射，文件名为topic，内容为id
                topicNameToTopicId.put(topic, topicId = (byte) topicCount.getAndDecrement());
                FileOutputStream fos = new FileOutputStream(new File(DISC_ROOT, topic));
                fos.write(topicId);
                fos.flush();
                fos.close();
            } else {
                // 文件存在，topic不在内存，从文件恢复
                FileInputStream fis = new FileInputStream(new File(DISC_ROOT, topic));
                topicId = (byte) fis.read();
            }
        }
        return topicId;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        ArrayList<long[]> position = metaInfo.get(topic).get(queueId);
        ByteBuffer readerBuffer = ByteBuffer.allocateDirect(17 * 1024);
        HashMap<Integer, ByteBuffer> ret = new HashMap<>();
        synchronized (position) {
            for (int i = (int) offset; i < (int) (offset + fetchNum) && i < position.size(); ++i) {
                long[] p = position.get(i);
                ByteBuffer buf = ByteBuffer.allocate((int) p[1]);

                ret.put(i, buf);
            }
        }
        return ret;
    }

    private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue) {
        V retObj = map.get(key);
        if (retObj != null) {
            return retObj;
        }

        map.put(key, defaultValue);
        return defaultValue;
    }

    public static void main(String[] args) throws IOException {
        FileChannel open = FileChannel.open(new File("aaa.txt").toPath(), StandardOpenOption.APPEND, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileChannel channel = open;
        ByteBuffer buf = ByteBuffer.allocate(128);
        buf.put("hello world".getBytes());
        buf.flip();
        int write = channel.write(buf);
        buf.position(0);
        channel.write(buf);
        channel.force(true);
        System.out.println(channel.position());

    }


}

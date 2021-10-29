package io.openmessaging;


import java.nio.ByteBuffer;
import java.util.Map;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class PowerFailureTest {
    public static void main(String[] args) throws InterruptedException {

        DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
//        int ab = 0;
//        ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo = mq.metaInfo;
//        for (Byte topicId : metaInfo.keySet()) {
//            ConcurrentHashMap<Short, QueueInfo> topicInfo = metaInfo.get(topicId);
//            for (Short queueId : topicInfo.keySet()) {
//                QueueInfo queueInfo = topicInfo.get(queueId);
//                if(queueInfo.size() != 300){
//                    int a = 3;
//                }
//                for (int offset = 0; offset < queueInfo.size(); offset++) {
//                    long[] p = queueInfo.getDataPosInFile(offset);
//                    short dataLen = (short) p[1];
//                    if (dataLen != 8){
//                        ab ++;
//                    }
//                }
//            }
//        }
//        System.out.println("ab: " + ab);
//        System.out.println("metaInfo done.");


        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int threadIndex = i;
            threads[i] = new Thread(() -> {
                for (int topicIndex = 0; topicIndex < topicCountPerThread; topicIndex++) {
                    String topic = threadIndex + "-" + topicIndex;
                    for (int queueIndex = 0; queueIndex < queueIdCountPerTopic; queueIndex++) {
                        for (int t = 0; t < writeTimesPerQueueId; t++) {
                            Map<Integer, ByteBuffer> res = mq.getRange(topic, queueIndex, t, 1);
                            ByteBuffer byteBuffer = res.get(0);
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
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        System.out.println("query done.");

//        Map<Integer, ByteBuffer> res = mq.getRange("10-1", 3, 0, 20);
//        System.out.println(res.get(0).getInt());
//        System.out.println(res.get(1).getInt());
    }
}

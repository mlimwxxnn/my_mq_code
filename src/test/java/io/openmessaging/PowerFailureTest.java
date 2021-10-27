package io.openmessaging;

import io.openmessaging.info.QueueInfo;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class PowerFailureTest {
    public static void main(String[] args) throws InterruptedException {

        DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();

        System.out.println(mq.getTotalFileSize());
        int ab = 0;
        ConcurrentHashMap<Byte, ConcurrentHashMap<Short, QueueInfo>> metaInfo = mq.metaInfo;
        for (Byte topicId : metaInfo.keySet()) {
            ConcurrentHashMap<Short, QueueInfo> topicInfo = metaInfo.get(topicId);
            for (Short queueId : topicInfo.keySet()) {
                QueueInfo queueInfo = topicInfo.get(queueId);
                if(queueInfo.size() != 300){
                    int a = 3;
                }
                for (int offset = 0; offset < queueInfo.size(); offset++) {
                    long[] p = queueInfo.getDataPosInFile(offset);
                    short dataLen = (short) p[1];
                    if (dataLen != 8){
                        ab ++;
                    }
                }
            }
        }
        System.out.println(ab);
        System.out.println("metaInfo done.");


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
                            if (topicIndex == 0 && queueIndex == 3 && t == 158 && "9-0".equals(topic)){
                                int a = 1;
                            }
                            if (byteBuffer.remaining() != 8){
                                System.out.println(String.format("remaining error: %d: %d: %d, topic: %s, remaining: %d", topicIndex, queueIndex, t, topic, byteBuffer.remaining()));
                            }
                            if (byteBuffer.getInt() != t || byteBuffer.getInt() != queueIndex){
                                System.out.println(String.format("data error: %d: %d: %d topic: %s", topicIndex, queueIndex, t, topic));
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
        System.out.println("done.");

//        Map<Integer, ByteBuffer> res = mq.getRange("10-1", 3, 0, 20);
//        System.out.println(res.get(0).getInt());
//        System.out.println(res.get(1).getInt());
    }
}


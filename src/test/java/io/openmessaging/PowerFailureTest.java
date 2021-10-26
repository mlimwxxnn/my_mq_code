package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class PowerFailureTest {
    public static void main(String[] args) throws InterruptedException {

        DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();

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
                            if (byteBuffer.getInt() != t || byteBuffer.getInt() != queueIndex){
                                System.out.println(String.format("error: %d: %d: %d", topicIndex, queueIndex, t));
                                System.out.println(topic);
                                System.exit(-1);
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


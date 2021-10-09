package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

public class PowerFailureTest {
    public static void main(String[] args) throws InterruptedException {
        DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
        Map<Integer, ByteBuffer> res = mq.getRange("2-0", 1, 0, 20);
        System.out.println(res.get(3).getInt());
        System.out.println(res.get(3).getInt());
    }
}


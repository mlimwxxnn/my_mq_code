package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

public class PowerFailureTest {
    public static void main(String[] args) throws InterruptedException {
        DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
        Map<Integer, ByteBuffer> res = mq.getRange("11", 15, 5, 100);
        System.out.println("");

    }
}


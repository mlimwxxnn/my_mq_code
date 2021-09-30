package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        DefaultMessageQueueImpl inst = new DefaultMessageQueueImpl();
        Map<Integer, ByteBuffer> ret = inst.getRange("abcmnp", 1000, 0, 100);
        System.out.println("");
    }
}


package io.openmessaging;

import com.intel.pmem.llpl.Heap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;


public class Test {

    public static void main(String args[]) throws InterruptedException, IOException {
        QueueInfo qi = new QueueInfo();
        qi.set(499, 34, 56);
        qi.set(500, 33, 56);
        qi.set(1000, 3, 8);
        System.out.println(qi.size());
        System.out.println(Arrays.toString(qi.get(499)));
        System.out.println(Arrays.toString(qi.get(500)));
    }
}

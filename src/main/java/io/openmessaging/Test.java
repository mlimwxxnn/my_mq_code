package io.openmessaging;

import com.intel.pmem.llpl.Heap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;


public class Test {

    public static void main(String args[]) throws InterruptedException, IOException {
        LinkedBlockingQueue<Integer> q = new LinkedBlockingQueue<>();
        for (int i = 0; i < 10; i++) {
            q.offer(i);
        }
        List<Integer> l = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            System.out.println(q.drainTo(l, 3));
            System.out.println(l);
        }
    }
}

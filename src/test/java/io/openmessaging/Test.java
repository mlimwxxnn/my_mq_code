package io.openmessaging;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.ArrayQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import static io.openmessaging.DefaultMessageQueueImpl.PMEM_ROOT;


public class Test {

    static void f(int a, int b){
        System.out.println(a);
        System.out.println(b);
    }

    public static void main(String[] args) {
        ArrayQueue<Integer> q = new ArrayQueue(2);
        q.put(3);
        System.out.println(q.isEmpty());
        System.out.println(q.isFull());
        q.put(23);
        System.out.println(q.isFull());
        System.out.println(q.put(2));
        System.out.println(q.get());
        System.out.println(q.get());
        q.put(3);
        System.out.println(q.get());
        System.out.println(q.size());
    }

}

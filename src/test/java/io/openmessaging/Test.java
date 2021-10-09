package io.openmessaging;

import com.intel.pmem.llpl.Heap;

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

    public static void main(String args[]) throws InterruptedException, IOException {
        Semaphore a = new Semaphore(0);
        a.release(10);
        a.acquire();
        a.acquire();
        System.out.println(a.tryAcquire(8));
        a.acquire();
        System.out.println("hello world");

    }
}
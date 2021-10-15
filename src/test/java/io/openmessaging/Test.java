package io.openmessaging;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.ArrayQueue;
import io.openmessaging.util.UnsafeUtil;
import jdk.nashorn.internal.ir.Block;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import static io.openmessaging.DefaultMessageQueueImpl.PMEM_ROOT;


public class Test {

    static void f(int a, int b){
        System.out.println(a);
        System.out.println(b);
    }

    public static void main(String[] args) throws InterruptedException {

    }

}

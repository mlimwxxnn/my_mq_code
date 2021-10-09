package io.openmessaging;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.info.QueueInfo;

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
        QueueInfo queueInfo = new QueueInfo();
        queueInfo.setDataPosInFile(200, 3, 2);
        System.out.println("hel");
    }
}

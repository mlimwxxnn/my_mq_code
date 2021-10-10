package io.openmessaging;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.info.PmemPageInfo;
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
        new Thread(()->{
            queueInfo.setDataPosInFile(200, 3, 2);
        }).start();
        new Thread(()->{
            queueInfo.setDataPosInPmem(200, new PmemPageInfo[]{new PmemPageInfo((byte) 5, 2)});
        }).start();
        System.out.println("hel");
    }
}

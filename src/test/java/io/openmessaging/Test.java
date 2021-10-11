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

//    public static void main(String args[]) throws InterruptedException, IOException {
//        for (int i = 0; i < 10000; i++) {
//            testQueueInfo();
//        }
//    }

//    public static void testQueueInfo(){
//        QueueInfo queueInfo = new QueueInfo();
//        Thread t1 = new Thread(()->{
//            queueInfo.setDataPosInFile(100, 3, 2);
//        });
//        Thread t2 = new Thread(()->{
//            queueInfo.setDataPosInPmem(100, new PmemPageInfo[]{new PmemPageInfo((byte) 5, 2)});
//        });
//        t1.start();
//        t2.start();
//        try {
//            t1.join();
//            t2.join();
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//    }
}

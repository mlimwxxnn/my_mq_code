package io.openmessaging;

import io.openmessaging.data.MyBlockingQueue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        // offer
        // poll take poll(timeout )


        int capacity = 15000000;
        int threadCount = 50;
        LinkedBlockingQueue<Integer> a = new LinkedBlockingQueue<>();
        MyBlockingQueue<Integer> b = new MyBlockingQueue<>(capacity);
        ArrayBlockingQueue<Integer> c = new ArrayBlockingQueue<>(capacity);
        CountDownLatch countDownLatch = new CountDownLatch(2 * threadCount);
        long start = System.currentTimeMillis();
        BlockingQueue<Integer> use = c;
        for (int i = 0; i < threadCount; i++) {
            new Thread(()->{
                for (int j = 0; j < capacity / threadCount; j++) {
                    use.offer(j);
                }
                countDownLatch.countDown();
            }).start();
            new Thread(()->{
                Integer take = 0;
                for (int j = 0; j < capacity / threadCount; j++) {
                    try {
                        take = use.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println(take);
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
        b.offer(3);
        long end = System.currentTimeMillis();
        System.out.printf("时间：%d", end -start);
    }
}

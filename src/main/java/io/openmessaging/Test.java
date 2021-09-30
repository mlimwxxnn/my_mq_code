package io.openmessaging;

import sun.misc.Unsafe;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

public class Test {
    public static void main(String[] args) {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(10);
        Semaphore semaphore = new Semaphore(10);
        semaphore.getQueueLength();
        semaphore.release(semaphore.getQueueLength());


        Unsafe unsafe = UnsafeUtil.unsafe;

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                    System.out.println("get in park");
                    unsafe.park(true, System.currentTimeMillis() + 6000);
                    System.out.println("finish first park");
                    unsafe.park(true, System.currentTimeMillis() + 6000);
                    System.out.println("finish second park");
                    System.out.println("unparked");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        unsafe.unpark(thread);
        unsafe.unpark(thread);
        System.out.println("do unpark in main");

        try {
            System.out.println("main get in sleep");
            Thread.sleep(10 * 1000);
            System.out.println("main finish sleep");
        } catch (InterruptedException e) {

        }
    }
}
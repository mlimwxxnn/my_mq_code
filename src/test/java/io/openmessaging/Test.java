package io.openmessaging;

import io.openmessaging.data.MyBlockingQueue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.*;

public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        // offer
        // poll take poll(timeout )

        CyclicBarrier cyclicBarrier = new CyclicBarrier(3);
        new Thread(()->{
            try {
                cyclicBarrier.await(100, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                System.out.println("0 i");
            } catch (BrokenBarrierException e) {
                System.out.println("0 b");
            } catch (TimeoutException e) {
                System.out.println("0 t");
            }
        }).start();
        new Thread(()->{
            try {
                cyclicBarrier.await(1, TimeUnit.MILLISECONDS);

            } catch (InterruptedException e) {
                System.out.println("1， i");
            } catch (BrokenBarrierException e) {
                System.out.println("1 b");
            } catch (TimeoutException e) {
                System.out.println("1 t");
            }
        }).start();
    }
}

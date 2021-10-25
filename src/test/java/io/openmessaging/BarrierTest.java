package io.openmessaging;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BarrierTest {
    public static void main(String[] args) {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(100);
        new Thread(() -> {
            try {
                cyclicBarrier.await(2, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                System.out.println("thread 1 broken");
            } catch (TimeoutException e) {
                System.out.println("thread 1 timeout");
            }
        }).start();

        try {
            cyclicBarrier.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            System.out.println("thread 2 broken");
        } catch (TimeoutException e) {
            System.out.println("thread 2 timeout");
        }

    }
}

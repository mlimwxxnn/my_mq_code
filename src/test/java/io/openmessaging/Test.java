package io.openmessaging;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Test {
    public static void main(String[] args)  {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        try {
            cyclicBarrier.await(2, TimeUnit.SECONDS);
        } catch (Exception e) {

        }


        try {
            cyclicBarrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

    }
}

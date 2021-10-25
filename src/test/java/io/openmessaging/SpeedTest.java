package io.openmessaging;

import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicLong;

public class SpeedTest {
    public static void main(String[] args) {
        long a = 0;
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                System.exit(-1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        Unsafe unsafe = UnsafeUtil.unsafe;
        AtomicLong atomicLong = new AtomicLong();
        while (true){
            a++;
//            try {
//                Thread.sleep(0);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            unsafe.park(false, 1000);
            System.out.println(a);
            if (a % 10000 == 0){
                System.out.println(a);
            }
        }
    }
}

package io.openmessaging;

import sun.misc.Unsafe;

public class Test {
    public static void main(String[] args) {
        Unsafe unsafe = UnsafeUtil.unsafe;

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                    System.out.println("get in park");
                    unsafe.park(true, System.currentTimeMillis() + 6000);
                    System.out.println("unparked");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
//        unsafe.unpark(thread);
        System.out.println("do unpark in main");

        try {
            System.out.println("main get in sleep");
            Thread.sleep(10 * 1000);
            System.out.println("main finish sleep");
        } catch (InterruptedException e) {

        }
    }
}

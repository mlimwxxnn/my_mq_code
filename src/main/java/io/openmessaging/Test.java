package io.openmessaging;

public class Test {
    public static Boolean stop = false;
    public static void main(String args[]) throws InterruptedException {
        new Thread(() -> {

        });
        System.out.println(Thread.activeCount());
    }
}

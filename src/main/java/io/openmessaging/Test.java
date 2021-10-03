package io.openmessaging;

public class Test {
    public static Boolean stop = false;
    public static void main(String args[]) throws InterruptedException {
        Thread testThread = new Thread(){
            @Override
            public void run(){
                int i = 1;
                while(!stop){
                    //System.out.println("in thread: " + Thread.currentThread() + " i: " + i);
                    i++;
                }
                System.out.println("Thread stop i="+ i);
            }
        }
                ;
        testThread.start();
        Thread.sleep(1000);
        stop = true;
        System.out.println("now, in main thread stop is: " + stop);
        testThread.join();
    }
}

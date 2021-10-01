package io.openmessaging;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println(thread.isAlive());
        thread.start();

        Thread.sleep(500);
        System.out.println(thread.isAlive());
    }
}

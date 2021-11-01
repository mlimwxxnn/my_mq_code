package io.openmessaging;
import javax.sound.midi.Soundbank;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.DefaultMessageQueueImpl.*;
public class Test {
    public static void main(String[] args)  {
        System.out.println(Thread.activeCount());
        ExecutorService executor = Executors.newFixedThreadPool(5);
        System.out.println(Thread.activeCount());
        for (int i = 0; i < 10; i++) {
            executor.submit(() -> {
                System.out.println("thread id is: " + Thread.currentThread().getId());
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}

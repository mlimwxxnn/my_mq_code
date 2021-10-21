package io.openmessaging.data;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class MyBlockingQueue<T> {
    ArrayBlockingQueue<T> q1;
    ArrayBlockingQueue<T> q2;
    ArrayBlockingQueue<T> putQueue;
    ArrayBlockingQueue<T> getQueue;
    final Object lockObj = new Object();

    private void swapIdentity(){
        synchronized (lockObj){
            ArrayBlockingQueue<T> qTmp = this.getQueue;
            getQueue = putQueue;
            putQueue = qTmp;
        }
    }


    public MyBlockingQueue(int capacity){
        q1 = new ArrayBlockingQueue<>(capacity);
        q2 = new ArrayBlockingQueue<>(capacity);
        putQueue = q1;
        getQueue = q2;
    }

    public boolean offer(T t){
        boolean b;
        synchronized (lockObj){
            b = putQueue.offer(t);
        }
        return b;
    }

    public T poll(){
        T t = getQueue.poll();
        if (t == null){
            swapIdentity();
        }
        return getQueue.poll();
    }

    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        T t = getQueue.poll();
        if ( t== null){
            t = putQueue.poll(timeout, unit);
            swapIdentity();
        }
        return t;
    }

}

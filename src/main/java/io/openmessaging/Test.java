package io.openmessaging;

import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;

public class Test {
    public static void main(String[] args) {
        AtomicInteger atomicInteger = new AtomicInteger();
        atomicInteger.compareAndSet(0, 1);

        Unsafe unsafe = UnsafeUtil.unsafe;
        Integer integer = 5;
        unsafe.compareAndSwapInt(integer, 12, 5, 6);
        System.out.println(integer);
    }
}

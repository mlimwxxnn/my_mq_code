package io.openmessaging;

import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

public class Test {
    static volatile  int a = 0;
    public static void main(String[] args) {
        Integer integer0 = 0;
        Integer integer1 = 0;

        Unsafe unsafe = UnsafeUtil.unsafe;
        unsafe.compareAndSwapInt(integer0, 12, 0, 1);
        System.out.println(integer0);
        System.out.println(integer1);
        System.out.println("sout a: " + a);
        System.out.printf("printf a: %d\n", a);
        DefaultMessageQueueImpl.log.info("slf4j a: {}", a);
    }
}

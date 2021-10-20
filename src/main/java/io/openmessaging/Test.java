package io.openmessaging;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;
public class Test {
    static Unsafe unsafe = UnsafeUtil.unsafe;
    public static void main(String[] args) {
        int a = 0;
        Integer integer0 = 0;
        Integer integer1 = 0;
        unsafe.compareAndSwapInt(integer0, 12, 0, 1);
        System.out.println("integer0: " + integer0);
        System.out.println("integer1: " + integer1);
        System.out.println("integer1.intValue(): " + integer1.intValue() );
        System.out.println("sout a: " + a);
        System.out.printf("printf a: %d\n", a);
        DefaultMessageQueueImpl.log.info("slf4j a: {}", a);
        Integer integer2 = new Integer(0);
        System.out.println("integer2: " + integer2);

        Integer integer = 0;
        System.out.println("**** set after: " + integer);
    }
}

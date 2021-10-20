package io.openmessaging;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;
public class Test {
    static Unsafe unsafe = UnsafeUtil.unsafe;
    public static void main(String[] args) {
        System.out.println(unsafe.ARRAY_BYTE_INDEX_SCALE);// @
    }
}

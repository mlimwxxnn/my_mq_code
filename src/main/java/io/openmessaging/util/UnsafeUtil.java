package io.openmessaging.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtil {
    public static Unsafe unsafe;
    static {
        Field field = null;
        try {
            field = Unsafe.class.getDeclaredField("theUnsafe");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        field.setAccessible(true);
        try {
            unsafe = (Unsafe) field.get(null);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}

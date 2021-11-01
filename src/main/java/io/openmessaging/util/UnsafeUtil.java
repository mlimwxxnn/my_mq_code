package io.openmessaging.util;

import sun.misc.Unsafe;

import java.io.FileDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class UnsafeUtil {
    public static final Unsafe unsafe;
    private static Method pwrite0;
    private static Method pread0;


    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
// get write/read method
            Class<?> fdp = Class.forName("sun.nio.ch.FileDispatcherImpl");
            pwrite0 = getMethod(fdp, "pwrite0", FileDescriptor.class, long.class, int.class, long.class);
            pread0 = getMethod(fdp, "pread0", FileDescriptor.class, long.class, int.class, long.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


// Bundle reflection calls to get access to the given method
    private static Method getMethod(Class<?> cls, String name, Class<?>... params) throws Exception {
        Method m = cls.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }


    public static void pwrite(FileDescriptor fd, long address, int len, long position) {
        try {
            pwrite0.invoke(null, fd, address, len, position);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    public static void pread(FileDescriptor fd, long address, int len, long position) {
        try {
            pread0.invoke(null, fd, address, len, position);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}


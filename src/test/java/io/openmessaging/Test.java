package io.openmessaging;

import io.openmessaging.data.MyBlockingQueue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.*;

import static io.openmessaging.util.UnsafeUtil.unsafe;

public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        User user = new User();
        for (long i = 0; i < 4; i+=8) {
            System.out.printf("%s ", Long.toBinaryString(unsafe.getByte(user, i)));
        }


    }
}
class User{
    String name;
    int id;
}

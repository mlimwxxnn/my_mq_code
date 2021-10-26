package io.openmessaging;

import io.openmessaging.data.MyBlockingQueue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.*;

import static io.openmessaging.util.UnsafeUtil.unsafe;

public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println(Short.MAX_VALUE / (1024));

    }
}
class User{
    String name;
    int id;
}

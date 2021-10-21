package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Test {
    public static void main(String[] args) throws IOException {
        RandomAccessFile raf = new RandomAccessFile("test.txt", "rw");
        long start = System.nanoTime();
        raf.setLength(10L * 1024 * 1024 * 1024);
        long end = System.nanoTime();
        System.out.println(end - start);
        System.out.println(raf.length());
    }
}

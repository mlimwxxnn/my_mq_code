package io.openmessaging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Test {
    public static void main(String[] args) throws Exception {
        RandomAccessFile file = new RandomAccessFile("hello", "rw");
        FileChannel channel = file.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        channel.write(byteBuffer);
        System.out.println(channel.position());
        System.out.println(channel.size());
    }
}

package io.openmessaging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Test {

    public static void main(String args[]) throws InterruptedException, IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append("hello\n");
        }
        File test_fileChannel = new File("test.txt");
        ByteBuffer buf = ByteBuffer.allocateDirect(1024*60);
        buf.put(sb.toString().getBytes());
        buf.flip();
        FileChannel channel = FileChannel.open(test_fileChannel.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        channel.write(buf);
        buf.position(5);
        buf.limit(6);
        buf.put((byte) 97);

        channel.force(true);

    }
}

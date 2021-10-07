package io.openmessaging;

import com.intel.pmem.llpl.Heap;

import java.io.IOException;
import java.nio.ByteBuffer;


public class Test {

    public static void main(String args[]) throws InterruptedException, IOException {
        ByteBuffer buf = ByteBuffer.allocate(128);
        buf.array()[0] = 97;
        buf.limit(1);
        buf.position(0);
        System.out.println(buf.get());
    }
}

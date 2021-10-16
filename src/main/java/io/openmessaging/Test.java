package io.openmessaging;

import java.nio.ByteBuffer;

public class Test {
    public static void main(String[] args) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(20);
        byte[] array = byteBuffer.array();
        System.out.println(array.length);
    }
}

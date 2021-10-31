package io.openmessaging;
import java.nio.ByteBuffer;

import static io.openmessaging.DefaultMessageQueueImpl.*;
public class Test {
    public static void main(String[] args)  {

        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.limit(1);

        buffer.get();
        buffer.get();
    }
}

package io.openmessaging.data;

import io.openmessaging.info.RamInfo;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class RamSaveSpaceData {
    private final int heapBufferCount = 4;
    private long directAlreadyAllocateSize = 0;
    private final int heapBufferSize = (int) (HEAP_CACHE_SIZE / heapBufferCount);
    private final ByteBuffer[] heapByteBuffers = new ByteBuffer[heapBufferCount];
    private final int[] heapAlreadyAllocateSizes = new int[heapBufferCount];
    ByteBuffer directByteBuffer;
    public static long directBufferBaseAddress;

    public RamSaveSpaceData() {
        for (int i = 0; i < heapBufferCount; i++) {
            heapByteBuffers[i] = ByteBuffer.allocate(heapBufferSize);
        }
        directByteBuffer = ByteBuffer.allocateDirect((int) DIRECT_CACHE_SIZE);
        directBufferBaseAddress = ((DirectBuffer) directByteBuffer).address();
    }

    public synchronized RamInfo allocate(int size){
        if (size < RAM_SPACE_LEVEL_GAP){
            size = RAM_SPACE_LEVEL_GAP;
        }
        if (directAlreadyAllocateSize + size < DIRECT_CACHE_SIZE){
            return allocateDirect(size);
        }
        return allocateHeap(size);
    }

    private RamInfo allocateHeap(int size){
        for (int heapBufferIndex = 0; heapBufferIndex < heapBufferCount; heapBufferIndex++) {
            int allocateSize = heapAlreadyAllocateSizes[heapBufferIndex];
            if (allocateSize + size > heapBufferSize){
                continue;
            }
            RamInfo ramInfo = new RamInfo(heapByteBuffers[heapBufferIndex].array(), allocateSize + 16, size);
            heapAlreadyAllocateSizes[heapBufferIndex] += size;
            return ramInfo;
        }
        return null;
    }
    private RamInfo allocateDirect(int size){
        RamInfo ramInfo = new RamInfo(null, directBufferBaseAddress + directAlreadyAllocateSize, size);
        directAlreadyAllocateSize += size;
        return ramInfo;
    }
}

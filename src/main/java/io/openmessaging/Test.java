package io.openmessaging;

import com.intel.pmem.llpl.Heap;

import java.io.IOException;


public class Test {

    public static void main(String args[]) throws InterruptedException, IOException {
        String option = "0";
        byte[] input;
        int size;  // String length

        // Define Heap
        boolean initialized = Heap.exists("/root/persistent_heap");
        Heap h = initialized ? Heap.openHeap("/root/persistent_heap") : Heap.createHeap("/root/persistent_heap", 8388608L);
        System.out.println("llpl可以使用");
    }
}

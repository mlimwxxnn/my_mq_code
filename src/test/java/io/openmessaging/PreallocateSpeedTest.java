package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;

public class PreallocateSpeedTest {
    public static final int groupCount = 5;
    public static final int testWriteBufferSize = 8 * 10 * 1024 + 200;
    public static final int preAllocateBufferSize = 100 * 4 * 1024;
    public static final long testTotalWriteFileSize = 1L * 1024 * 1024 * 1024;

    public static FileChannel[] openFileChannel(boolean isPreAllocate){
        FileChannel[] dataWriteChannels = new FileChannel[groupCount];
        File rootFile;
        if (isPreAllocate){
            rootFile = new File("d:/essd/preAllocate");
        }else {
            rootFile = new File("d:/essd/noAllocate");
        }
        // 删除掉原来写过的测试文件
        if (! rootFile.exists()){
            rootFile.mkdirs();
        }
        for (File file : rootFile.listFiles()) {
            if(file.isFile()){
                file.delete();
            }else{
                for (File listFile : file.listFiles()) {
                    if (listFile.isFile()){
                        listFile.delete();
                    }
                }
            }
        }
        for (int i = 0; i < dataWriteChannels.length; i++) {
            File file = new File(rootFile, "data-" + i);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                dataWriteChannels[i] = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return dataWriteChannels;
    }

    public static void testWriteChannel(FileChannel channel){
        ByteBuffer buffer = ByteBuffer.allocateDirect(testWriteBufferSize);
        buffer.position(testWriteBufferSize);
        for (long t = 0; t < testTotalWriteFileSize / ((groupCount - 1) * testWriteBufferSize); t++) {
            try {
                long position = channel.position();
                channel.position(((position + 4 * 1024 - 1) >> 12) << 12); // position对 4k 向上取整
                buffer.flip();
                channel.write(buffer);
                channel.force(true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void preAllocate(FileChannel[] dataWriteChannels){
        CountDownLatch allocateCountDownLatch = new CountDownLatch(groupCount - 1);
        for (int i = 0; i < groupCount - 1; i++) {
            final int groupId = i;
            new Thread(() -> {
                System.out.println("填充："+groupId);
                try {
                    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(preAllocateBufferSize);
                    // MAX_MESSAGE_FILE_SIZE: 125G, groupCount: 5
                    byteBuffer.position(preAllocateBufferSize);
                    for (long writeTime = 0; writeTime < (testTotalWriteFileSize << 1) / ((groupCount - 1) * preAllocateBufferSize) ; writeTime++) {
                        byteBuffer.flip();
                        dataWriteChannels[groupId].write(byteBuffer);
                        dataWriteChannels[groupId].force(true);
                    }
                    dataWriteChannels[groupId].force(true);
                    dataWriteChannels[groupId].position(0);
                    allocateCountDownLatch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println(groupId + " 填充完毕");
            }).start();
        }
        try {
            allocateCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static long noAllocate() throws Exception {
        FileChannel[] dataWriteChannels = openFileChannel(false);
        Thread[] threads = new Thread[groupCount - 1];
        for (int i = 0; i < threads.length; i++) {
            final int groupId = i;
            threads[i] = new Thread(() -> {
                System.out.println("未填充直接写入: " + groupId);
                testWriteChannel(dataWriteChannels[groupId]);
                System.out.println("未填充直接写入完毕: " + groupId);
            });
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        long stop = System.currentTimeMillis();
        return stop - start;
    }


    public static long preAllocate() throws Exception {
        FileChannel[] dataWriteChannels = openFileChannel(true);
        // ssd预先填0
        preAllocate(dataWriteChannels);
        Thread[] threads = new Thread[groupCount - 1];
        for (int i = 0; i < threads.length; i++) {
            final int groupId = i;
            threads[i] = new Thread(() -> {
                System.out.println("填充后写入: " + groupId);
                testWriteChannel(dataWriteChannels[groupId]);
                System.out.println("填充后写入完毕: " + groupId);
            });
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        long stop = System.currentTimeMillis();
        return stop - start;
    }

    public static void main(String[] args) {
        int testTimes = 3;
        new Thread(() -> {
            for (int i = 0; i < testTimes; i++) {
                try {
                    long timeCost;
                    timeCost = noAllocate();
                    System.out.println("no allocate one time: " + timeCost);
                    timeCost = preAllocate();
                    System.out.println("pre allocate one time: " + timeCost);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }).start();
    }
}

package io.openmessaging.writer;

import com.intel.pmem.llpl.TransactionalMemoryBlock;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static io.openmessaging.writer.PmemDataWriter.getBlockByAllocateAndSetData;

public class ReLoader {
    private final BlockingQueue<ByteBuffer> loadedDataBufferQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<ByteBuffer> freeDataBufferQueue = new LinkedBlockingQueue<>();
//    private final AtomicInteger reloadFinishedChannelCount = new AtomicInteger();

    public ReLoader() {
        for (int i = 0; i < RELOAD_BUFFER_COUNT; i++) {
            ByteBuffer dataBuffer = ByteBuffer.allocate(RELOAD_BUFFER_SIZE);
            freeDataBufferQueue.offer(dataBuffer);
        }
    }

    public void reload(){
        readData();
        saveData();
    }

    public void saveData(){
        byte topicId;
        short queueId;
        short dataLen;
        int offset;
        QueueInfo queueInfo;
        TransactionalMemoryBlock block;
        try {
            while (true){
                ByteBuffer dataBuffer = loadedDataBufferQueue.take();
                while (dataBuffer.hasRemaining()){
                    topicId = dataBuffer.get();
                    queueId = dataBuffer.getShort();
                    dataLen = dataBuffer.getShort();
                    offset = dataBuffer.getInt();

                    queueInfo = metaInfo.get(topicId).get(queueId);
                    while (!queueInfo.willNotToQuery(offset)) {
                        if ((block = getBlockByAllocateAndSetData(dataBuffer, dataLen)) == null){
                            Thread.sleep(1);
                        } else {
                            queueInfo.setDataPosInPmem(offset, new PmemPageInfo(block));
                            break;
                        }
                    }
                    dataBuffer.position(dataBuffer.position() + dataLen);
                }
                freeDataBufferQueue.offer(dataBuffer);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void readData() {
        log.info("reload start");
        CountDownLatch countDownLatch = new CountDownLatch(SSD_WRITE_THREAD_COUNT);
        for (int i = 0; i < SSD_WRITE_THREAD_COUNT; i++) {
            final int id = i;
            new Thread(() -> {
                try {
                    FileChannel channel = FileChannel.open(Paths.get("/essd", "data-" + id), StandardOpenOption.READ);
                    channel.position(range[id][0]);
                    short dataLen;
                    while (channel.position() < range[id][1]){
                        ByteBuffer dataBuffer = freeDataBufferQueue.take();
                        dataBuffer.clear();
                        int readLen = (int) Math.min(range[id][1] - channel.position(), RELOAD_BUFFER_SIZE);
                        dataBuffer.limit(readLen);
                        channel.read(dataBuffer);
                        dataBuffer.flip();
                        while (dataBuffer.remaining() > DATA_INFORMATION_LENGTH){
                            dataLen = dataBuffer.getShort(dataBuffer.position() + 3);
                            if (dataBuffer.remaining() < dataLen + DATA_INFORMATION_LENGTH){
                                break;
                            }
                            dataBuffer.position(dataBuffer.position() + dataLen + DATA_INFORMATION_LENGTH);
                        }
                        channel.position(channel.position() - dataBuffer.remaining());
                        dataBuffer.limit(dataBuffer.position());
                        dataBuffer.position(0);
                        loadedDataBufferQueue.offer(dataBuffer);
                    }
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        try {
            countDownLatch.await();
            log.info("reload finish read");
//            System.exit(-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

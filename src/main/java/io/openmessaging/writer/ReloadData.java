package io.openmessaging.writer;

import com.intel.pmem.llpl.TransactionalMemoryBlock;
import io.openmessaging.data.WrappedData;
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

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static io.openmessaging.writer.PmemDataWriter.getBlockByAllocateAndSetData;

public class ReloadData {
    private Unsafe unsafe = UnsafeUtil.unsafe;
    public final BlockingQueue<WrappedData> reloadWrappedDataQueue = new LinkedBlockingQueue<>(50);

    public ReloadData() {
        readData();
        saveData();
    }

    public void saveData(){
//        TransactionalMemoryBlock block;
//        topicId = dataBuffer.get();
//        queueId = dataBuffer.getShort();
//        dataLen = dataBuffer.getShort();
//        offset = dataBuffer.getInt();
//        queueInfo = metaInfo.get(topicId).get(queueId);
        //                            while (!queueInfo.willNotToQuery(offset)) {
//                                if ((block = getBlockByAllocateAndSetData(dataBuffer, dataLen)) == null)
//                                    Thread.sleep(1);
//                                else {
//                                    if(offset >= queueInfo.size()){
//                                        System.out.println("错误：重新加载数据使queueInfo扩容了");
//                                    }
//                                    queueInfo.setDataPosInPmem(offset, new PmemPageInfo(block));
//                                    break;
//                                }
//                            }
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
                    int dataBufferSize = 4 * 1024 * 1024;
                    ByteBuffer dataBuffer = ByteBuffer.allocate(dataBufferSize);
                    byte topicId;
                    short queueId;
                    short dataLen;
                    int offset = 0;
                    QueueInfo queueInfo;
                    while (channel.position() < range[id][1]){
                        dataBuffer.clear();
                        int readLen = (int) Math.min(range[id][1] - channel.position(), dataBufferSize);
                        dataBuffer.limit(readLen);
                        channel.read(dataBuffer);
                        dataBuffer.flip();
                        while (dataBuffer.remaining() > DATA_INFORMATION_LENGTH){
                            dataLen = dataBuffer.getShort(3);
                            if (dataBuffer.remaining() < dataLen + DATA_INFORMATION_LENGTH){
                                break;
                            }
                            dataBuffer.position(dataBuffer.position() + dataLen + DATA_INFORMATION_LENGTH);
                        }
                        channel.position(channel.position() - dataBuffer.remaining());
                    }
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        try {
            countDownLatch.await();
            log.info("reload finish");
            System.exit(-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

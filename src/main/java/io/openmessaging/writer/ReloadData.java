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
import java.util.HashMap;

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static io.openmessaging.writer.PmemDataWriter.getBlockByAllocateAndSetData;

public class ReloadData {
    private Unsafe unsafe = UnsafeUtil.unsafe;

    public ReloadData() {
        readData();
    }

    public void readData() {
        for (int i = 0; i < SSD_WRITE_THREAD_COUNT; i++) {
            final int id = i;
            new Thread(() -> {
                try {
                    FileChannel channel = null;

                    channel = FileChannel.open(Paths.get("/essd", "data-" + id), StandardOpenOption.READ);
                    channel.position(range[id][0]);
//                    ByteBuffer infoBuffer = ByteBuffer.allocate(DATA_INFORMATION_LENGTH);
                    int dataBufferSize = 4 * 1024 * 1024;
                    ByteBuffer dataBuffer = ByteBuffer.allocate(dataBufferSize);
                    byte topicId;
                    short queueId;
                    short dataLen;
                    int offset;
                    HashMap<Short, QueueInfo> topicInfo;
                    QueueInfo queueInfo;
                    while (channel.position() < range[id][1]){
                        dataBuffer.clear();
                        int readLen = (int) Math.min(range[id][1] - channel.position(), dataBufferSize);
                        dataBuffer.limit(readLen);
                        channel.read(dataBuffer);
                        dataBuffer.flip();
                        while (dataBuffer.remaining() > DATA_INFORMATION_LENGTH){
                            topicId = dataBuffer.get();
                            queueId = dataBuffer.getShort();
                            dataLen = dataBuffer.getShort();
                            offset = dataBuffer.getInt();
                            if (dataBuffer.remaining() < dataLen){
                                dataBuffer.position(dataBuffer.position() - DATA_INFORMATION_LENGTH);
                                break;
                            }
                            topicInfo = metaInfo.computeIfAbsent(topicId, k -> new HashMap<>(2000));
                            queueInfo = topicInfo.computeIfAbsent(queueId, k -> new QueueInfo());
                            TransactionalMemoryBlock block;
                            while (!queueInfo.willNotToQuery(offset)) {
                                if ((block = getBlockByAllocateAndSetData(dataBuffer, dataLen)) == null)
                                    Thread.sleep(1);
                                else {
                                    queueInfo.setDataPosInPmem(offset, new PmemPageInfo(block));
                                    break;
                                }
                            }
                            dataBuffer.position(dataBuffer.position() + dataLen);
                        }
                        channel.position(channel.position() - dataBuffer.remaining());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

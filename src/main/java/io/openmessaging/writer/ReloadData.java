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
                    ByteBuffer dataBuffer = ByteBuffer.allocate(4 * 1024 * 1024);
                    byte topicId;
                    short queueId;
                    short dataLen;
                    int offset;
                    HashMap<Short, QueueInfo> topicInfo;
                    QueueInfo queueInfo;
                    int remaining = 0;
                    while (channel.position() < range[id][1]) {
                        unsafe.copyMemory(dataBuffer.array(), 16 + dataBuffer.position(), dataBuffer.array(), 16, remaining);
                        dataBuffer.position(remaining);
                        int readLen = (int) Math.min(range[id][1] - channel.position(), 4 * 1024 * 1024 - remaining);
                        dataBuffer.limit(remaining + readLen);
                        channel.read(dataBuffer);
                        dataBuffer.flip();
                        int cur = 0;
                        while(dataBuffer.position() < dataBuffer.limit()) {
                            topicId = dataBuffer.get();
                            queueId = dataBuffer.getShort();
                            dataLen = dataBuffer.getShort();
                            offset = dataBuffer.getInt();

                            topicInfo = metaInfo.computeIfAbsent(topicId, k -> new HashMap<>(2000));
                            queueInfo = topicInfo.computeIfAbsent(queueId, k -> new QueueInfo());




                            TransactionalMemoryBlock block;
                            while (!queueInfo.willNotToQuery(offset)) {
                                if ((block = getBlockByAllocateAndSetData(ByteBuffer.wrap(dataBuffer.array(), dataBuffer.position(), dataLen))) == null)
                                    Thread.sleep(1);
                                else {
                                    queueInfo.setDataPosInPmem(offset, new PmemPageInfo(block));
                                    break;
                                }
                            }
                            dataBuffer.position(dataBuffer.position() + dataLen);
                            remaining = dataBuffer.limit() - dataBuffer.position();
                            if(remaining < 109 || Short.reverseBytes(unsafe.getShort(dataBuffer.array(), 16 + dataBuffer.position() + 3)) > remaining - DATA_INFORMATION_LENGTH) {
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

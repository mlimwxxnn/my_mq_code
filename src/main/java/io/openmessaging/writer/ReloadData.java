package io.openmessaging.writer;

import com.intel.pmem.llpl.TransactionalMemoryBlock;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static io.openmessaging.writer.PmemDataWriter.getBlockByAllocateAndSetData;

public class ReloadData {

    public ReloadData() {
        readData();
    }

    public void readData() {
        for (int i = 0; i < SSD_WRITE_THREAD_COUNT; i++) {
            final int id = i;
            new Thread(()-> {
                try {
                FileChannel channel = null;

                channel = FileChannel.open(Paths.get("/essd", "data-" + id), StandardOpenOption.READ);
                channel.position(range[id][0]);
                ByteBuffer infoBuffer = ByteBuffer.allocate(DATA_INFORMATION_LENGTH);
                ByteBuffer dataBuffer = ByteBuffer.allocate(17 * 1024);
                byte topicId;
                short queueId;
                short dataLen = 0;
                int offset;
                HashMap<Short, QueueInfo> topicInfo;
                QueueInfo queueInfo;
                while (channel.position() < range[id][1]) {
                    // read meta info
                    infoBuffer.clear();
                    channel.read(infoBuffer);
                    infoBuffer.flip();
                    topicId = infoBuffer.get();
                    queueId = infoBuffer.getShort();
                    dataLen = infoBuffer.getShort();
                    offset = infoBuffer.getInt();

                    // read data
                    dataBuffer.position(0);
                    dataBuffer.limit(dataLen);
                    channel.read(dataBuffer);
                    dataBuffer.flip();

                    topicInfo = metaInfo.computeIfAbsent(topicId, k -> new HashMap<>(2000));
                    queueInfo = topicInfo.computeIfAbsent(queueId, k -> new QueueInfo());

                    TransactionalMemoryBlock block;
                    if(!queueInfo.willNotToQuery(offset)) {
                        while ((block = getBlockByAllocateAndSetData(dataBuffer)) == null) {
                            Thread.sleep(1);
                        }
                        queueInfo.setDataPosInPmem(offset, new PmemPageInfo(block));
                    }
                }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

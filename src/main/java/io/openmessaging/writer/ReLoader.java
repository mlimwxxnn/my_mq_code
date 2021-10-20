package io.openmessaging.writer;

import com.intel.pmem.llpl.TransactionalMemoryBlock;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static io.openmessaging.writer.PmemDataWriter.getBlockByAllocateAndSetData;

public class ReLoader {
    private ByteBuffer[] reloadByteBuffers = new ByteBuffer[SSD_WRITE_THREAD_COUNT];

    public ReLoader() {
        for (int i = 0; i < SSD_WRITE_THREAD_COUNT; i++) {
            reloadByteBuffers[i] = ByteBuffer.allocateDirect(RELOAD_BUFFER_SIZE);
        }
    }

    public void reload() {
        for (int i = 0; i < SSD_WRITE_THREAD_COUNT; i++) {
            final int id = i;
            new Thread(() -> {
                try {
                    long readPosition = range[id][0];
                    log.info("channel-{} reload start", id);
                    FileChannel channel = dataWriteChannels[id];
                    ByteBuffer dataBuffer = reloadByteBuffers[id];
                    long dataBufferAddress = ((DirectBuffer) dataBuffer).address();
                    byte topicId;
                    short queueId;
                    short dataLen;
                    int offset;
                    QueueInfo queueInfo;
                    TransactionalMemoryBlock block;
                    while (readPosition < range[id][1]){
                        dataBuffer.clear();
                        int readLen = (int) Math.min(range[id][1] - readPosition, RELOAD_BUFFER_SIZE);
                        dataBuffer.limit(readLen);
                        channel.read(dataBuffer, readPosition);
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
                            queueInfo = metaInfo.get(topicId).get(queueId);
                            while (!queueInfo.willNotToQuery(offset)) {
                                if ((block = getBlockByAllocateAndSetData(null, dataBufferAddress + dataBuffer.position() , dataLen)) != null){
                                    queueInfo.setDataPosInPmem(offset, new PmemPageInfo(block));
                                    break;
                                } else {
                                    Thread.sleep(1);
                                }
                            }
                            dataBuffer.position(dataBuffer.position() + dataLen);
                        }
                        readPosition += readLen - dataBuffer.remaining();
                    }
                    log.info("channel-{} reload finish", id);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

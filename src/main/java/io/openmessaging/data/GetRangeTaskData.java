package io.openmessaging.data;

import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.openmessaging.DefaultMessageQueueImpl.pmemDataWriter;
import static io.openmessaging.writer.PmemDataWriterV2.freePmemPageQueues;
//import static io.openmessaging.writer.PmemDataWriter.memoryBlocks;


public class GetRangeTaskData {
    public final ByteBuffer[] buffers = new ByteBuffer[100]; // 用来响应查询的buffer
    private Map<Integer, ByteBuffer> result = new HashMap<>();
    String topic;
    int queueId;
    long offset;
    int fetchNum;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public GetRangeTaskData() {
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocate(17 * 1024);
        }
    }

    public void setGetRangeParameter(String topic, int queueId, long offset, int fetchNum) {
        this.topic = topic;
        this.queueId = queueId;
        this.offset = offset;
        this.fetchNum = fetchNum;
    }

    public Map<Integer, ByteBuffer> getResult() {
        countDownLatch = new CountDownLatch(1);
        return result;
    }

    public void queryData() {
        try {
            result.clear();
            Byte topicId = DefaultMessageQueueImpl.getTopicId(topic, false);
            if (topicId == null) {
                return;
            }
            QueueInfo queueInfo = DefaultMessageQueueImpl.metaInfo.get(topicId).get((short) queueId);
            if (queueInfo == null) {
                return;
            }
            if(!queueInfo.haveQueried()){
                PmemPageInfo[] allPmemPageInfos = queueInfo.getAllPmemPageInfos();
                for (int j = 0; j < offset - 1; j++) {
                    PmemPageInfo pmemPageInfo = allPmemPageInfos[j];
                    if(pmemPageInfo != null) {
                        freePmemPageQueues[pmemPageInfo.freePmemPageQueueIndex].offer(pmemPageInfo);
                    }
                }
            }
            int tmp, n = fetchNum < (tmp = queueInfo.size() - (int) offset) ? fetchNum : tmp;
            int dataLen;
            for (int i = 0; i < n; i++) {
                long[] p = queueInfo.getDataPosInFile(i + (int) offset);
                dataLen = (int) p[1];
                ByteBuffer buf = buffers[i];
                buf.clear();
                buf.limit(dataLen);
                if (!queueInfo.isInPmem(i + (int) offset)) {
                    int id = (int) (p[1] >> 32);
                    DefaultMessageQueueImpl.dataWriteChannels[id].read(buf, p[0]);
                    buf.flip();
                } else {
                    PmemPageInfo pmemPageInfo = queueInfo.getDataPosInPmem(i + (int) offset);
                    byte[] bufArray = buf.array();

                    pmemPageInfo.block.copyToArray(0, bufArray, 0, dataLen);

                    freePmemPageQueues[pmemPageInfo.freePmemPageQueueIndex].offer(pmemPageInfo);
                }
                result.put(i, buf);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }
}


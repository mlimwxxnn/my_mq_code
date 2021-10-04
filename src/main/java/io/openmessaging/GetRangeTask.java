package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


class GetRangeTask {
    public final ByteBuffer[] buffers = new ByteBuffer[100]; // 用来响应查询的buffer
    private Map<Integer, ByteBuffer> result = new HashMap<>();
    String topic; int queueId; long offset; int fetchNum;
    public CountDownLatch countDownLatch = new CountDownLatch(1);

    public GetRangeTask(){
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocateDirect(17 * 1024);
        }
    }
    void setGetRangeParameter(String topic, int queueId, long offset, int fetchNum){
        this.topic = topic;
        this.queueId = queueId;
        this.offset = offset;
        this.fetchNum = fetchNum;
    }

    Map<Integer, ByteBuffer> getResult(){
        countDownLatch = new CountDownLatch(1);
        return result;
    }

    public void queryData(){
        try {
            result.clear();

            Byte topicId = DefaultMessageQueueImpl.getTopicId(topic, false);
            if (topicId == null){
                return;
            }

            HashMap<Integer, long[]> queueInfo = DefaultMessageQueueImpl.metaInfo.get(topicId).get((short)queueId);
            if(queueInfo == null){
                return ;
            }

            for (int i = 0; i < fetchNum && (i + offset) < queueInfo.size(); i++) {
                long[] p = queueInfo.get(i + (int) offset);
                ByteBuffer buf = buffers[i];
                buf.clear();
                buf.limit((int) p[1]);
                int id = (int) (p[1] >> 32);
                DefaultMessageQueueImpl.dataWriteChannels[id].read(buf, p[0]);
                buf.flip();
                result.put(i, buf);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}


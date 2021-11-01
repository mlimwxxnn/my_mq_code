package io.openmessaging.data;

import io.openmessaging.DefaultMessageQueueImpl;
//import io.openmessaging.info.PmemInfo;
import io.openmessaging.info.DataPosInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.info.RamInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.openmessaging.DefaultMessageQueueImpl.metaInfo;
import static io.openmessaging.info.QueueInfo.IN_PMEM;
import static io.openmessaging.info.QueueInfo.IN_RAM;
import static io.openmessaging.writer.PmemDataWriter.freePmemQueues;
import static io.openmessaging.data.PmemSaveSpaceData.*;
import static io.openmessaging.writer.RamDataWriter.*;
import static io.openmessaging.DefaultMessageQueueImpl.log;

// todo 这里把初始化改到MQ构造函数里会更快
public class GetRangeTaskData {
    public final ByteBuffer[] buffers = new ByteBuffer[100]; // 用来响应查询的buffer
    private Map<Integer, ByteBuffer> result = new HashMap<>();
    String topic;
    int queueId;
    long offset;
    int fetchNum;
    Unsafe unsafe = UnsafeUtil.unsafe;

    public GetRangeTaskData() {
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocateDirect(17 * 1024);
        }

    }

    public void setGetRangeParameter(String topic, int queueId, long offset, int fetchNum) {
        this.topic = topic;
        this.queueId = queueId;
        this.offset = offset;
        this.fetchNum = fetchNum;
    }

    public Map<Integer, ByteBuffer> getResult() {
        return result;
    }

    public void queryData() {
        try {
            result.clear();

            Byte topicId = DefaultMessageQueueImpl.getTopicId(topic, false);
            if (topicId == null) {
                return;
            }
            QueueInfo queueInfo = null;
            try {
                queueInfo = DefaultMessageQueueImpl.metaInfo.get(topicId).get((short) queueId);
            }catch (Exception e){
                log.info("metaInfo.get(): {}", metaInfo.get(topicId));
            }
            if (queueInfo == null) {
                return;
            }

            if (!queueInfo.haveQueried()) {
                queueInfo.deleteBefore((int)offset);
            }

            int n = Math.min(fetchNum, queueInfo.size() - (int) offset);

            short dataLen;
            for (int i = 0; i < n; i++) {
                ByteBuffer buf = buffers[i];
                buf.clear();
                DataPosInfo dataPosition = queueInfo.getDataPosition();
                switch (dataPosition.getState()){ // 判断数据位置再读取
                    case IN_RAM:
                        RamInfo ramInfo = dataPosition.getDataPosInRam();
                        buf.limit(ramInfo.dataLen);
                        unsafe.copyMemory(ramInfo.ramObj, ramInfo.offset, null, ((DirectBuffer) buf).address(), ramInfo.dataLen);
                        freeRamQueues[ramInfo.levelIndex].offer(ramInfo); // 回收
                        break;
                    case IN_PMEM:
                        long pmemInfo = dataPosition.getDataPosInPmem();
                        int pmemChannelIndex = (byte)(pmemInfo>>>40);
                        dataLen = (short)(pmemInfo >>> 48);
                        buf.limit(dataLen);
                        pmemChannels[pmemChannelIndex].read(buf, pmemInfo & 0xff_ffff_ffffL);
                        freePmemQueues[pmemChannelIndex].offer(pmemInfo & 0xffff_ffff_ffffL); // 回收
                        buf.flip();
                        break;
                    default:
                        long[] p = dataPosition.getDataPosInFile();
                        int id = (int) (p[1] >> 32);
                        buf.limit((int)(p[1]));
                        DefaultMessageQueueImpl.dataWriteChannels[id].read(buf, p[0]);
                        buf.flip();
                        break;
                }
                result.put(i, buf);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}


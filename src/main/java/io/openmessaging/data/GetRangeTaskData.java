package io.openmessaging.data;

import com.intel.pmem.llpl.TransactionalMemoryBlock;
import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.info.PmemInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.info.RamInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static io.openmessaging.writer.PmemDataWriter.freePmemQueues;
import static io.openmessaging.data.PmemSaveSpaceData.*;
import static io.openmessaging.writer.RamDataWriter.*;

// todo 这里把初始化改到MQ构造函数里会更快
public class GetRangeTaskData {
    public final ByteBuffer[] buffers = new ByteBuffer[100]; // 用来响应查询的buffer
    private Map<Integer, ByteBuffer> result = new HashMap<>();
    String topic;
    int queueId;
    long offset;
    int fetchNum;
    private CountDownLatch countDownLatch;
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
        this.countDownLatch = new CountDownLatch(1);
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
            QueueInfo queueInfo = DefaultMessageQueueImpl.metaInfo.get(topicId).get((short) queueId);
            if (queueInfo == null) {
                return;
            }

            if (!queueInfo.haveQueried()) {
                PmemInfo[] allPmemInfos = queueInfo.getAllPmemPageInfos();
                for (int j = 0; j < offset; j++) {
                    PmemInfo pmemInfo = allPmemInfos[j];
                    if (pmemInfo != null) {
                        freePmemQueues[pmemInfo.rLevelIndex].offer(pmemInfo);
                    }
                    if(queueInfo.isInRam(j)){
                        RamInfo ramInfo = queueInfo.getDataPosInRam();
                        freeRamQueues[ramInfo.levelIndex].offer(ramInfo);
                    }
                }
            }

            int n = Math.min(fetchNum, queueInfo.size() - (int) offset);

            short dataLen;
            for (int i = 0; i < n; i++) {
                int currentOffset = i + (int) offset;
                long[] p = queueInfo.getDataPosInFile(currentOffset);
                dataLen = (short) p[1];
                ByteBuffer buf = buffers[i];
                buf.clear();
                buf.limit(dataLen);
                if(queueInfo.isInRam(currentOffset)){
                    RamInfo ramInfo = queueInfo.getDataPosInRam();
                    unsafe.copyMemory(ramInfo.ramObj, ramInfo.offset, null, ((DirectBuffer) buf).address(), dataLen); //direct
                    freeRamQueues[ramInfo.levelIndex].offer(ramInfo);

                }else
                if (queueInfo.isInPmem(currentOffset)) {
                    PmemInfo pmemInfo = queueInfo.getDataPosInPmem(currentOffset);
                    int pmemChannelIndex = pmemInfo.rLevelIndex;
                    pmemChannels[pmemChannelIndex].read(buf, pmemInfo.address);
                    freePmemQueues[pmemInfo.rLevelIndex].offer(pmemInfo); // 回收
                    buf.flip();
                } else {
                    int id = (int) (p[1] >> 32);
                    DefaultMessageQueueImpl.dataWriteChannels[id].read(buf, p[0]);
                    buf.flip();
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


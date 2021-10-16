package io.openmessaging.data;

import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.openmessaging.DefaultMessageQueueImpl.*;
import static io.openmessaging.writer.RamDataWriter.*;
import static java.lang.System.arraycopy;


public class GetRangeTaskData {
    public final ByteBuffer[] buffers = new ByteBuffer[100]; // 用来响应查询的buffer
    private Map<Integer, ByteBuffer> result = new HashMap<>();
    private Unsafe unsafe = UnsafeUtil.unsafe;
    String topic;
    int queueId;
    long offset;
    int fetchNum;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public GetRangeTaskData() {
        for (int i = 0; i < buffers.length; i++) {
            // todo 这里改成directByteBuffer能提高ssd的read速度， by wl
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
                        pmemPageInfo.block.free();
                    }
                }
            }
            int tmp, n = fetchNum < (tmp = queueInfo.size() - (int) offset) ? fetchNum : tmp;
            short dataLen;
            for (int i = 0; i < n; i++) {
                int currentOffset = i + (int) offset;
                long[] p = queueInfo.getDataPosInFile(currentOffset);
                dataLen = (short) p[1];
                ByteBuffer buf = buffers[i];
                buf.clear();
                buf.limit(dataLen);
//                if(queueInfo.isInRam(currentOffset)){
//                    long queryStart = System.nanoTime();
//
//                    Integer address = queueInfo.getDataPosInRam();
//                    int ramBufferIndex = getIndexByDataLength(dataLen);
//                    unsafe.copyMemory(null, ((DirectBuffer)ramBuffers[ramBufferIndex]).address() + address, buf.array(), 16, dataLen);//direct
////                    arraycopy(ramBuffers[ramBufferIndex].array(), address, buf.array(), 0, dataLen);//heap
//                    freeRamQueues[ramBufferIndex].offer(address);
//
//                    // 统计信息
//                    long queryStop = System.nanoTime();
//                    if (GET_READ_TIME_COST_INFO){
//                        readTimeCostCount.addRamTimeCost(queryStop - queryStart);
//                    }
//                    if (GET_CACHE_HIT_INFO){
//                        hitCountData.increaseRamHitCount();
//                    }
//                }else
                    if(queueInfo.isInPmem(currentOffset)) {
                    long queryStart = System.nanoTime();

                    PmemPageInfo pmemPageInfo = queueInfo.getDataPosInPmem(currentOffset);
                    byte[] bufArray = buf.array();
                    pmemPageInfo.block.copyToArray(0, bufArray, 0, dataLen);
                    pmemPageInfo.block.free();

                    // 统计信息
                    long queryStop = System.nanoTime();
                    if (GET_READ_TIME_COST_INFO){
                        readTimeCostCount.addPmemTimeCost(queryStop - queryStart);
                    }
                    if (GET_CACHE_HIT_INFO){
                        hitCountData.increasePmemHitCount();
                    }
                }else {
                    long queryStart = System.nanoTime();

                    int id = (int) (p[1] >> 32);
                    DefaultMessageQueueImpl.dataWriteChannels[id].read(buf, p[0]);
                    buf.flip();

                    // 统计信息
                    long queryStop = System.nanoTime();
                    if (GET_READ_TIME_COST_INFO){
                        readTimeCostCount.addSsdTimeCost(queryStop - queryStart);
                    }
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


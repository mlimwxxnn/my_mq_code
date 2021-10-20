package io.openmessaging.data;

import com.intel.pmem.llpl.MemoryAccessor;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.info.PmemPageInfo;
import io.openmessaging.info.QueueInfo;
import io.openmessaging.util.ArrayQueue;
import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.DefaultMessageQueueImpl.*;

// todo 这里把初始化改到MQ构造函数里会更快
public class GetRangeTaskData {
    public final ByteBuffer[] buffers = new ByteBuffer[100]; // 用来响应查询的buffer
    public final TransactionalMemoryBlock[] blocks = new TransactionalMemoryBlock[100];
    int toFreeBlockCount = 0;
    private Map<Integer, ByteBuffer> result = new HashMap<>();
    String topic;
    int queueId;
    long offset;
    int fetchNum;
    private CountDownLatch countDownLatch;
    Unsafe unsafe = UnsafeUtil.unsafe;
    long bufferCapacityOffset;
    long bufferAddressOffset;
    long blockAddressOffset;
    ArrayQueue<ByteBuffer> freeDataBuffers = new ArrayQueue<>(100);
    ArrayQueue<ByteBuffer> toFreeBuffers = new ArrayQueue<>(100);

    {
        try {
            bufferCapacityOffset = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            bufferAddressOffset = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            blockAddressOffset = unsafe.objectFieldOffset(MemoryAccessor.class.getDeclaredField("directAddress"));
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    public GetRangeTaskData() {
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocateDirect(1);
            unsafe.freeMemory(unsafe.getLong(buffers[i], bufferAddressOffset));
            unsafe.putInt(buffers[i], bufferCapacityOffset, 17 * 1024);
            freeDataBuffers.put(ByteBuffer.allocateDirect(17 * 1024));
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

            while (toFreeBlockCount > 0){
                blocks[--toFreeBlockCount].free();
            }
            while (toFreeBuffers.size() > 0) {
                freeDataBuffers.put(toFreeBuffers.get());
            }

            Byte topicId = DefaultMessageQueueImpl.getTopicId(topic, false);
            if (topicId == null) {
                return;
            }
            QueueInfo queueInfo = DefaultMessageQueueImpl.metaInfo.get(topicId).get((short) queueId);
            if (queueInfo == null) {
                return;
            }

            if (!queueInfo.haveQueried()) {
                PmemPageInfo[] allPmemPageInfos = queueInfo.getAllPmemPageInfos();
                for (int j = 0; j < offset; j++) {
                    PmemPageInfo pmemPageInfo = allPmemPageInfos[j];
                    queueInfo.setWillNotToQuery(j);
                    if (pmemPageInfo != null) {
                        pmemPageInfo.block.free();
                    }
                }
            }

            int n = Math.min(fetchNum, queueInfo.size() - (int) offset);

            short dataLen;
            for (int i = 0; i < n; i++) {
                int currentOffset = i + (int) offset;
                queueInfo.setWillNotToQuery(currentOffset);
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
                if (queueInfo.isInPmem(currentOffset)) {
                    long queryStart = System.nanoTime();

                    PmemPageInfo pmemPageInfo = queueInfo.getDataPosInPmem(currentOffset);
                    TransactionalMemoryBlock block = pmemPageInfo.block;
                    blocks[toFreeBlockCount++] = block;
                    long blockDirectAddress = unsafe.getLong(block, blockAddressOffset);
                    unsafe.putLong(buf, bufferAddressOffset, blockDirectAddress + 8); // 8 是block内部的metaSize大小


                    // 统计信息
                    long queryStop = System.nanoTime();
                    if (GET_READ_TIME_COST_INFO) {
                        readTimeCostCount.addPmemTimeCost(queryStop - queryStart);
                    }
                    if (GET_CACHE_HIT_INFO) {
                        hitCountData.increasePmemHitCount();
                    }
                } else {
                    long queryStart = System.nanoTime();

                    buf = freeDataBuffers.get();
                    buf.clear();
                    buf.limit(dataLen);
                    int id = (int) (p[1] >> 32);
                    DefaultMessageQueueImpl.dataWriteChannels[id].read(buf, p[0]);
                    buf.flip();

                    // 统计信息
                    long queryStop = System.nanoTime();
                    if (GET_READ_TIME_COST_INFO) {
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


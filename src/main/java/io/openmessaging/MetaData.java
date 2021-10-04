package io.openmessaging;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class MetaData {
    private final byte topicId;
    private final short queueId;
    private final int offset;
    private long offsetInMergedBuffer;
    private final short dataLen;
    private final HashMap<Integer, long[]> queueInfo;
    public final CountDownLatch countDownLatch = new CountDownLatch(1);

    public MetaData(byte topicId, short queueId, short dataLen, int offset, HashMap<Integer, long[]> queueInfo) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.dataLen = dataLen;
        this.offset = offset;
        this.queueInfo = queueInfo;
    }

    public byte getTopicId() {
        return topicId;
    }


    public short getQueueId() {
        return queueId;
    }

    public void setOffsetInMergedBuffer(long offsetInMergedBuffer) {
        this.offsetInMergedBuffer = offsetInMergedBuffer;
    }

    public long getOffsetInMergedBuffer() {
        return offsetInMergedBuffer;
    }

    public short getDataLen() {
        return dataLen;
    }

    public int getOffset() {
        return offset;
    }

    public HashMap<Integer, long[]> getQueueInfo() {
        return queueInfo;
    }
}

package io.openmessaging;

import java.util.concurrent.CountDownLatch;

public class MetaData {
    private final byte topicId;
    private final int queueId;
    private long offsetInMergedBuffer;
    private final short dataLen;
    public CountDownLatch countDownLatch = new CountDownLatch(1);

    public MetaData(byte topicId, int queueId, short dataLen) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.dataLen = dataLen;
    }

    public byte getTopicId() {
        return topicId;
    }


    public int getQueueId() {
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

}

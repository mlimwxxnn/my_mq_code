package io.openmessaging.data;

import io.openmessaging.info.QueueInfo;

import java.util.concurrent.CountDownLatch;

public class MetaData {
    private final byte topicId;
    private final short queueId;
    private final int offset;
    private long offsetInMergedBuffer;
    private final short dataLen;
    private final QueueInfo queueInfo;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    public MetaData(byte topicId, short queueId, short dataLen, int offset, QueueInfo queueInfo) {
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

    public QueueInfo getQueueInfo() {
        return queueInfo;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }
}

package io.openmessaging.data;

import io.openmessaging.info.QueueInfo;

import java.util.concurrent.CountDownLatch;

public class MetaData {
    private  byte topicId;
    private  short queueId;
    private  int offset;
    private long offsetInMergedBuffer;
    private  short dataLen;
    private  QueueInfo queueInfo;
    private  CountDownLatch countDownLatch;

    public MetaData() {

    }

    public void setMetaInfo(byte topicId, short queueId, short dataLen, int offset, QueueInfo queueInfo){
        this.topicId = topicId;
        this.queueId = queueId;
        this.dataLen = dataLen;
        this.offset = offset;
        this.queueInfo = queueInfo;
        this.countDownLatch = new CountDownLatch(1);
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

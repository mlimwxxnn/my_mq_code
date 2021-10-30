package io.openmessaging.data;

import io.openmessaging.info.QueueInfo;

import java.util.concurrent.CountDownLatch;

public class MetaData {
    private  int offset;
    private  short dataLen;
    private  QueueInfo queueInfo;
    private  CountDownLatch countDownLatch;

    public MetaData() {

    }

    public void setMetaInfo(short dataLen, int offset, QueueInfo queueInfo){
        this.dataLen = dataLen;
        this.offset = offset;
        this.queueInfo = queueInfo;
        this.countDownLatch = new CountDownLatch(1);
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

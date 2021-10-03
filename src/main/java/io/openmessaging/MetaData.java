package io.openmessaging;

public class MetaData {
    private byte topicId;
    private int queueId;
    private Thread thread;
    private long offsetInMergedBuffer;
    private short dataLen;

    public MetaData(byte topicId, int queueId, Thread thread, short dataLen) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.thread = thread;
        this.dataLen = dataLen;
    }

    public byte getTopicId() {
        return topicId;
    }


    public int getQueueId() {
        return queueId;
    }

    public Thread getThread() {
        return thread;
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

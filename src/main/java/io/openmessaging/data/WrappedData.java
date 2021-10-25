package io.openmessaging.data;

import io.openmessaging.info.QueueInfo;

import java.nio.ByteBuffer;

public class WrappedData {
    private final MetaData meta;

    private  ByteBuffer data;

    public WrappedData() {
        this.meta = new MetaData();
    }

    public MetaData getMeta() {
        return meta;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setWrapInfo(byte topicId, short queueId, ByteBuffer data, int offset, QueueInfo queueInfo){
        this.meta.setMetaInfo(topicId, queueId, (short)data.remaining(), offset, queueInfo);
        this.data = data;
    }
}

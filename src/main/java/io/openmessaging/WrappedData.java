package io.openmessaging;

import java.nio.ByteBuffer;

public class WrappedData {
    private MetaData meta;

    private ByteBuffer data;


    public WrappedData(byte topicId, int queueId, Thread thread, ByteBuffer data) {
        this.meta = new MetaData(topicId, queueId, thread, (short)data.remaining());
        this.data = data;

    }

    public MetaData getMeta() {
        return meta;
    }

    public ByteBuffer getData() {
        return data;
    }
}

package io.openmessaging;

import java.nio.ByteBuffer;

public class WrappedData {
    private final MetaData meta;

    private final ByteBuffer data;


    public WrappedData(byte topicId, int queueId, ByteBuffer data) {
        this.meta = new MetaData(topicId, queueId, (short)data.remaining());
        this.data = data;

    }

    public MetaData getMeta() {
        return meta;
    }

    public ByteBuffer getData() {
        return data;
    }
}

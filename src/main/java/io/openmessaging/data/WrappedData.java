package io.openmessaging.data;

import io.openmessaging.info.QueueInfo;

import java.nio.ByteBuffer;

public class WrappedData {
    private final MetaData meta;

    private final ByteBuffer data;

    public WrappedData(byte topicId, short queueId, ByteBuffer data, int offset, QueueInfo queueInfo) {
        this.meta = new MetaData(topicId, queueId, (short)data.remaining(), offset, queueInfo);
        this.data = data;

    }

    public MetaData getMeta() {
        return meta;
    }

    public ByteBuffer getData() {
        return data;
    }
}

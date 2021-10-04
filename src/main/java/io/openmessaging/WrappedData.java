package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class WrappedData {
    private final MetaData meta;

    private final ByteBuffer data;

    public WrappedData(byte topicId, short queueId, ByteBuffer data, int offset, HashMap<Integer, long[]> queueInfo) {
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

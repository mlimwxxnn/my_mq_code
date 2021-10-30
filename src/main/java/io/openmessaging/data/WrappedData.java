package io.openmessaging.data;

import io.openmessaging.info.QueueInfo;

import java.nio.ByteBuffer;

public class WrappedData {
    private final MetaData meta;

    private  ByteBuffer data;

    private int dataPosition;

    public WrappedData() {
        this.meta = new MetaData();
    }

    public MetaData getMeta() {
        return meta;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setWrapInfo(ByteBuffer data, int offset, QueueInfo queueInfo){
        this.meta.setMetaInfo((short)data.remaining(), offset, queueInfo);
        this.data = data;
        this.dataPosition = data.position();
    }

    public int getDataPosition() {
        return dataPosition;
    }
}

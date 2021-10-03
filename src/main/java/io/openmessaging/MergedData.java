package io.openmessaging;


import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class MergedData {

    final Unsafe unsafe = UnsafeUtil.unsafe;
    private final HashSet<MetaData> metaSet = new HashSet<>();  // todo 这里可以设置一个初始容量
    private final ByteBuffer mergedBuffer;
    private int count = 0;


    public MergedData(ByteBuffer buffer) {
        buffer.clear();
        this.mergedBuffer = buffer;
    }

    public void putData(WrappedData data) {
        count++;
        // 放入数据的元信息，7字节
        MetaData meta = data.getMeta();
        metaSet.add(meta);
        mergedBuffer.put(meta.getTopicId());
        mergedBuffer.putInt(meta.getQueueId());
        int dataLen = meta.getDataLen();
        mergedBuffer.putShort((short)dataLen);
        meta.setOffsetInMergedBuffer(mergedBuffer.position());

        // 放入数据本体
        unsafe.copyMemory(data.getData().array(), 16 + data.getData().position(), null, ((DirectBuffer)mergedBuffer).address() + mergedBuffer.position(), dataLen);
        mergedBuffer.position(mergedBuffer.position() + dataLen);
    }

    public int getCount() {
        return this.count;
    }

    public ByteBuffer getMergedBuffer(){
        this.mergedBuffer.flip();
        return this.mergedBuffer;
    }

    public HashSet<MetaData> getMetaSet() {
        return metaSet;
    }
}

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

    public void putData(WrappedData wrappedData) {
        count++;
        // 放入数据的元信息，9 字节
        MetaData meta = wrappedData.getMeta();
        metaSet.add(meta);

        mergedBuffer.put(meta.getTopicId());
        mergedBuffer.putShort(meta.getQueueId());
        short dataLen = meta.getDataLen();
        mergedBuffer.putShort(dataLen);
        mergedBuffer.putInt(wrappedData.getMeta().getOffset());
        meta.setOffsetInMergedBuffer(mergedBuffer.position());

        // 放入数据本体
        unsafe.copyMemory(wrappedData.getData().array(), 16 + wrappedData.getData().position(), null, ((DirectBuffer)mergedBuffer).address() + mergedBuffer.position(), dataLen);
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

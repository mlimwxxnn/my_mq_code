package io.openmessaging.data;


import io.openmessaging.util.UnsafeUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MergedData {

    final Unsafe unsafe = UnsafeUtil.unsafe;
    private final List<MetaData> metaList = new ArrayList<>(50);
    private final ByteBuffer mergedBuffer;
    private int count = 0;

    public void reset(){
        this.count = 0;
        this.metaList.clear();
        this.mergedBuffer.clear();
    }

    public MergedData(ByteBuffer buffer) {
        this.mergedBuffer = buffer;
    }

    public void putData(WrappedData wrappedData) {
        count++;
        MetaData meta = wrappedData.getMeta();
        metaList.add(meta);

        // 放入数据的元信息，9 字节
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

    public void putAllData(List<WrappedData> wrappedDataList) {
        wrappedDataList.forEach(this::putData);
    }

    public int getCount() {
        return this.count;
    }

    public ByteBuffer getMergedBuffer(){
        this.mergedBuffer.flip();
        return this.mergedBuffer;
    }

    public List<MetaData> getMetaSet() {
        return metaList;
    }
}

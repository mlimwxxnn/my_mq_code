package io.openmessaging;

import static java.lang.System.arraycopy;

public class QueueInfo {
    private long[][] dataInfo;
    private int maxIndex;
    private int capacity;
    private static final int DEFAULT_CAPACITY = 100;
    public QueueInfo(){
        this(DEFAULT_CAPACITY);
    }

    public QueueInfo(int initialCapacity){
        dataInfo = new long[initialCapacity][2];
        maxIndex = -1;
        capacity = initialCapacity;
    }

    private void ensureCapacity(int index){
        if(index >= capacity){
            capacity = index * 2;
            long[][] newDataInfo = new long[capacity][2];
            arraycopy(dataInfo, 0, newDataInfo, 0, maxIndex + 1);
            this.dataInfo = newDataInfo;
        }
    }

    public void set(int i, long fileChannelOffset, long fileIdAndLen){
        ensureCapacity(i);
        if(i > maxIndex){
            maxIndex = i;
        }
        dataInfo[i][0] = fileChannelOffset;
        dataInfo[i][1] = fileIdAndLen;
    }

    public long[] get(int i){
        if(i > maxIndex){
            throw new IndexOutOfBoundsException("索引越界");
        }
        return dataInfo[i];
    }

    public int size(){
        return maxIndex + 1;
    }

}

package io.openmessaging.info;




import io.openmessaging.util.ArrayQueue;

import java.util.Queue;

import static java.lang.System.arraycopy;

public class QueueInfo {
//    private PageNode[];
    private int maxIndex;
    private volatile long[][] dataInfo;
    private volatile int capacity;
    private volatile byte[] status; // 0 表示数据在ssd中 1 表示数据在pmem中 2 表示数据在内存中
    private volatile PmemPageInfo[] pmemPageInfos;
    private boolean haveQueried;
    private final ArrayQueue<Integer> dataPosInRam = new ArrayQueue<>(2);
    private static final int DEFAULT_CAPACITY = 100;

    public QueueInfo(){
        this(DEFAULT_CAPACITY);
    }

    public QueueInfo(int initialCapacity){
        dataInfo = new long[initialCapacity][2];
        status = new byte[initialCapacity];
        pmemPageInfos = new PmemPageInfo[initialCapacity];
        maxIndex = -1;
        capacity = initialCapacity;
    }

    private void ensureCapacity(int index){
        if(index >= capacity){
            synchronized (this) {
                if (index >= capacity) {
                    int newCapacity = index * 2;
                    long[][] newDataInfo = new long[newCapacity][2];
                    byte[] newStatus = new byte[newCapacity];
                    PmemPageInfo[] newPmemPageInfos = new PmemPageInfo[newCapacity];

                    arraycopy(dataInfo, 0, newDataInfo, 0, maxIndex + 1);
                    arraycopy(status, 0, newStatus, 0, maxIndex + 1);
                    arraycopy(pmemPageInfos, 0, newPmemPageInfos, 0, maxIndex + 1);

                    this.dataInfo = newDataInfo;
                    this.status = newStatus;
                    this.pmemPageInfos = newPmemPageInfos;
                    capacity = newCapacity;
                }
            }
        }
    }

    public void setDataPosInRam(int i, int address) {
        ensureCapacity(i);
        if(i > maxIndex){
            maxIndex = i;
        }
        if(dataPosInRam.put(address)) {
            status[i] |= 2;
        }
    }

    public Integer getDataPosInRam(){
        return dataPosInRam.get();
    }

//    public void setDataPosInPmem(int i, PmemPageInfo[] pmemPageInfo){
//        ensureCapacity(i);
//        if(i > maxIndex){
//            maxIndex = i;
//        }
//        pmemPageInfos[i] = pmemPageInfo;
//        isInPmem[i] = true;
//    }

    public void setPmemBlockMemory(int i, PmemPageInfo pmemPageInfo){
        ensureCapacity(i);
        if(i > maxIndex){
            maxIndex = i;
        }
        pmemPageInfos[i] = pmemPageInfo;
        status[i] |= 1;
    }

    public boolean isInPmem(int i){
        return (status[i] & 1) > 0;
    }


    public void setDataPosInFile(int i, long fileChannelOffset, long fileIdAndLen){
        ensureCapacity(i);
        if(i > maxIndex){
            maxIndex = i;
        }
        dataInfo[i][0] = fileChannelOffset;
        dataInfo[i][1] = fileIdAndLen;
    }

    public long[] getDataPosInFile(int i){
        if(i > maxIndex){
            throw new IndexOutOfBoundsException("索引越界");
        }
        return dataInfo[i];
    }

    public int size(){
        return maxIndex + 1;
    }

    public PmemPageInfo getDataPosInPmem(int i) {
        if(i > maxIndex){
            throw new IndexOutOfBoundsException("索引越界");
        }
        return pmemPageInfos[i];
    }

    public PmemPageInfo[] getAllPmemPageInfos() {
        haveQueried = true;
        return pmemPageInfos;
    }

    public boolean haveQueried(){
        return haveQueried;
    }
}

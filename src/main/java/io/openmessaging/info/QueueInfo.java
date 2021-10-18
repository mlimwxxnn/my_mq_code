package io.openmessaging.info;


import io.openmessaging.util.ArrayQueue;
import static java.lang.System.arraycopy;

public class QueueInfo {
//    private PageNode[];
    private int maxIndex;
    private volatile long[][] dataInfo;
    private volatile int capacity;
    // 末位为 1 表示数据在pmem中，倒数第二位为 1 表示数据在内存中，倒数第三位为 1 表示此offset的数据不会再被查
    private volatile byte[] status;
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

    private void updateMaxIndex(int i) {
        if(i > maxIndex) {
            maxIndex = i;
        }
    }
    public void setDataPosInRam(int i, int address) {
        ensureCapacity(i);
        dataPosInRam.put(address);
        synchronized (this){
            status[i] |= 2;
        }
    }

    public Integer getDataPosInRam(){
        return dataPosInRam.get();
    }


    public boolean ramIsFull(){
        return dataPosInRam.isFull();
    }

    public void setDataPosInPmem(int i, PmemPageInfo pmemPageInfo){
        ensureCapacity(i);
        synchronized (this){
            pmemPageInfos[i] = pmemPageInfo;
            status[i] |= 1;
        }
    }

    public boolean isInPmem(int i){
        return (status[i] & 1) > 0;
    }


    public void setDataPosInFile(int i, long fileChannelOffset, long fileIdAndLen){
        ensureCapacity(i);
        updateMaxIndex(i);
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

    public boolean isInRam(int i) {
        return (status[i] & 2) > 0;
    }

    public void setWillNotToQuery(int i) {
        synchronized (this) {
            status[i] |= 4;
        }
    }

    public boolean willNotToQuery(int i) {
        boolean b;
        synchronized (this){
            b = (status[i] & 4) > 0;
        }
        return b;
    }
}

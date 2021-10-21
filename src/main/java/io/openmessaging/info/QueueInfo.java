package io.openmessaging.info;


import io.openmessaging.util.ArrayQueue;
import static java.lang.System.arraycopy;

// todo 这里尝试去掉多余的 volatile
public class QueueInfo {
    private volatile int maxIndex;
    private volatile long[][] dataInfo;
    private volatile int capacity;
    // 末位为 1 表示数据在pmem中，倒数第二位为 1 表示数据在内存中，倒数第三位为 1 表示此offset的数据不会再被查
    private volatile byte[] status;
    private volatile PmemInfo[] pmemInfos;
    private volatile boolean haveQueried;
    private final ArrayQueue<RamInfo> dataPosInRam = new ArrayQueue<>(80);  // todo 这里堆内用多少，要再试
    private static final int DEFAULT_CAPACITY = 100;

    public QueueInfo(){
        this(DEFAULT_CAPACITY);
    }

    public QueueInfo(int initialCapacity){
        dataInfo = new long[initialCapacity][2];
        status = new byte[initialCapacity];
        pmemInfos = new PmemInfo[initialCapacity];
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
                    PmemInfo[] newPmemInfos = new PmemInfo[newCapacity];

                    arraycopy(dataInfo, 0, newDataInfo, 0, maxIndex + 1);
                    arraycopy(status, 0, newStatus, 0, maxIndex + 1);
                    arraycopy(pmemInfos, 0, newPmemInfos, 0, maxIndex + 1);

                    this.dataInfo = newDataInfo;
                    this.status = newStatus;
                    this.pmemInfos = newPmemInfos;
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
    public void setDataPosInRam(int i, RamInfo ramInfo) {
        ensureCapacity(i);
        dataPosInRam.put(ramInfo);
        synchronized (this){
            status[i] |= 2;
        }
    }

    public RamInfo getDataPosInRam(){
        return dataPosInRam.get();
    }


    public boolean ramIsFull(){
        return dataPosInRam.isFull();
    }

    public boolean ramIsEmpty(){
        return dataPosInRam.isEmpty();
    }
    public void setDataPosInPmem(int i, PmemInfo pmemInfo){
        ensureCapacity(i);
        synchronized (this){
            pmemInfos[i] = pmemInfo;
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

    public PmemInfo getDataPosInPmem(int i) {
        if(i > maxIndex){
            throw new IndexOutOfBoundsException("索引越界");
        }
        return pmemInfos[i];
    }

    public PmemInfo[] getAllPmemPageInfos() {
        haveQueried = true;
        return pmemInfos;
    }


    public boolean haveQueried(){
        return haveQueried;
    }

    public boolean isInRam(int i) {
        return (status[i] & 2) > 0;
    }
}

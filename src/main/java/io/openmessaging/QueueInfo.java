package io.openmessaging;

import static java.lang.System.arraycopy;

public class QueueInfo {
    private long[][] dataInfo;
//    private PageNode[]; //
    private int maxIndex;
    private int capacity;
    private boolean isInPmem[];
    private PmemPageInfo[][] pmemPageInfos;
    private boolean haveQueried;
    private static final int DEFAULT_CAPACITY = 100;

    public QueueInfo(){
        this(DEFAULT_CAPACITY);
    }

    public QueueInfo(int initialCapacity){
        dataInfo = new long[initialCapacity][2];
        isInPmem = new boolean[initialCapacity];
        pmemPageInfos = new PmemPageInfo[initialCapacity][];
        maxIndex = -1;
        capacity = initialCapacity;
    }

    private void ensureCapacity(int index){ // todo 这里后面还要给其他数组扩容
        if(index >= capacity){
            capacity = index * 2;
            long[][] newDataInfo = new long[capacity][2];
            boolean[] newIsInPmem = new boolean[capacity];
            PmemPageInfo[][] newPmemPageInfos = new PmemPageInfo[capacity][];

            arraycopy(dataInfo, 0, newDataInfo, 0, maxIndex + 1);
            arraycopy(isInPmem, 0, newIsInPmem, 0, maxIndex + 1);
            arraycopy(pmemPageInfos, 0, newPmemPageInfos, 0, maxIndex + 1);

            this.dataInfo = newDataInfo;
            this.isInPmem = newIsInPmem;
            this.pmemPageInfos = newPmemPageInfos;
        }
    }

    public void setDataPosInPmem(int i, PmemPageInfo[] pmemPageInfo){
        ensureCapacity(i);
        pmemPageInfos[i] = pmemPageInfo;
        isInPmem[i] = true;
    }

    public boolean isInPmem(int i){
        return isInPmem[i];
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

    public PmemPageInfo[] getDataPosInPmem(int i) {
        return pmemPageInfos[i];
    }

    public PmemPageInfo[][] getAllPmemPageInfos() {
        haveQueried = true;
        return pmemPageInfos;
    }

    public boolean haveQueried(){
        return haveQueried;
    }
}

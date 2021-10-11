package io.openmessaging.info;

import com.intel.pmem.llpl.MemoryBlock;

public class PmemPageInfo {
    public int freePmemPageQueueIndex;
//    private byte blockId;
//    private int pageIndex;
//
//    public PmemPageInfo(byte blockId, int pageIndex) {
//        this.blockId = blockId;
//        this.pageIndex = pageIndex;
//    }
//
//    public byte getBlockId() {
//        return blockId;
//    }
//
//    public void setBlockId(byte blockId) {
//        this.blockId = blockId;
//    }
//
//    public int getPageIndex() {
//        return pageIndex;
//    }
//
//    public void setPageIndex(int pageIndex) {
//        this.pageIndex = pageIndex;
//    }
    public MemoryBlock block;
}

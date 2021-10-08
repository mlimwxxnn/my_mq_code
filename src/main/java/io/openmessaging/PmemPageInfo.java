package io.openmessaging;

public class PmemPageInfo {
    private byte blockId;
    private int pageIndex;

    public PmemPageInfo(byte blockId, int pageIndex) {
        this.blockId = blockId;
        this.pageIndex = pageIndex;
    }

    public byte getBlockId() {
        return blockId;
    }

    public void setBlockId(byte blockId) {
        this.blockId = blockId;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }
}

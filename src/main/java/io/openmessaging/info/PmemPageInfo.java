package io.openmessaging.info;

import com.intel.pmem.llpl.TransactionalMemoryBlock;

public class PmemPageInfo {
    public PmemPageInfo(long address, int group){
        this.address = address;
        this.group = group;
    }
    public long address;
    public int group;
}

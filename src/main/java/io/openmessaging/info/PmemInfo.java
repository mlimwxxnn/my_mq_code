package io.openmessaging.info;

import com.intel.pmem.llpl.TransactionalMemoryBlock;

public class PmemInfo {
    public PmemInfo(long address, int group){
        this.address = address;
        this.group = group;
    }
    public long address;
    public int group;
}

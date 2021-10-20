package io.openmessaging.info;

import com.intel.pmem.llpl.TransactionalMemoryBlock;

public class PmemPageInfo {
    public PmemPageInfo(TransactionalMemoryBlock block){
        this.block = block;
    }
    public TransactionalMemoryBlock block;
}

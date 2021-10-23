package io.openmessaging.info;

public class PmemInfo {
    public PmemInfo(long address, int rLevelIndex){
        this.address = address;
        this.rLevelIndex = rLevelIndex;
    }
    public long address;
    public int rLevelIndex;  // means for: retrieveLevelIndex
}

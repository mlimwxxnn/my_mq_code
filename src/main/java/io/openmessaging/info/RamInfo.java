package io.openmessaging.info;

public class RamInfo {
    public Object ramObj;
    public long offset;
    public RamInfo(Object obj, long offset){
        this.ramObj = obj;
        this.offset = offset;
    }
}

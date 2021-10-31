package io.openmessaging.info;

public class DataPosInfo {
    private final byte state;
    private final Object dataPos;

    public DataPosInfo(byte state, Object dataPos){
        this.state = state;
        this.dataPos = dataPos;
    }
    public byte getState(){
        return state;
    }
    // fileChannelOffset, fileIdAndLen
    public long[][] getDataPosInFile(){
        return (long[][]) dataPos;
    }
    public Long getDaPosInPmem(){
        return (Long) dataPos;
    }
    public RamInfo getDataPosInRam(){
        return (RamInfo) dataPos;
    }
}

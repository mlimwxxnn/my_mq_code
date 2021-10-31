package io.openmessaging.info;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class RamInfo {
    public Object ramObj;
    public long offset;
    public int levelIndex;
    public int dataLen;

    public RamInfo(Object obj, long offset, int size){
        this.ramObj = obj;
        this.offset = offset;
        this.levelIndex = getRetrieveLevelIndexByDataLen(size);
    }

    public static int getRetrieveLevelIndexByDataLen(int dataLen){
        return dataLen / RAM_SPACE_LEVEL_GAP - 1;
    }

    public static int getEnoughFreeSpaceLevelIndexByDataLen(int dataLen){
        return (dataLen + RAM_SPACE_LEVEL_GAP - 1) / RAM_SPACE_LEVEL_GAP - 1;
    }
}

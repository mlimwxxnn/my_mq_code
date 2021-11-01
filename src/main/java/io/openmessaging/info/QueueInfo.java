package io.openmessaging.info;


import java.util.LinkedList;

import static io.openmessaging.writer.PmemDataWriter.freePmemQueues;
import static io.openmessaging.writer.RamDataWriter.freeRamQueues;

// todo 这里尝试去掉多余的 volatile
public class QueueInfo {
    public static final byte IN_RAM = (byte) 2;
    public static final byte IN_PMEM = (byte) 1;
    public static final byte IN_FILE = (byte) 0;
    private int queueLength;
    private boolean haveQueried;
    private final LinkedList<DataPosInfo> dataPos = new LinkedList<>();

    public DataPosInfo getDataPosition(){
        return dataPos.pollFirst();
    }
    public void setDataPosition(DataPosInfo dataPosInfo) {
        queueLength ++;
        dataPos.addLast(dataPosInfo);
    }
    public int size() {
        return queueLength;
    }

    public void deleteBefore(int i){
        for (int j = 0; j < i; j++) {
            DataPosInfo dataPosInfo = dataPos.pollFirst();
            RamInfo ramInfo;
            long pmemInfo;

            switch (dataPosInfo.getState()){
                case IN_RAM:
                    ramInfo = dataPosInfo.getDataPosInRam();
                    freeRamQueues[ramInfo.levelIndex].offer(ramInfo);
                    break;
                case IN_PMEM:
                    pmemInfo = dataPosInfo.getDataPosInPmem();
                    freePmemQueues[(byte)(pmemInfo>>>40)].offer(pmemInfo & 0xffff_ffff_ffffL); // 回收
                    break;
                default:
                    break;
            }
        }
    }

    public boolean haveQueried(){
        boolean old = haveQueried;
        haveQueried = true;
        return old;
    }
}

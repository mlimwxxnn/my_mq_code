package io.openmessaging.data;

//import io.openmessaging.info.PmemInfo;
import io.openmessaging.info.RamInfo;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static io.openmessaging.DefaultMessageQueueImpl.*;

public class PmemSaveSpaceData {
    public static final FileChannel[] pmemChannels = new FileChannel[spaceLevelCount];
    private static final long[] channelAlreadyAllocateSizes = new long[spaceLevelCount];
    private long alreadyAllocateSize = 0;

    public PmemSaveSpaceData() {
        RandomAccessFile[] rafs = new RandomAccessFile[spaceLevelCount];
        try {
            for (int queueIndex = 0; queueIndex < spaceLevelCount; queueIndex++) {
                rafs[queueIndex] = new RandomAccessFile(PMEM_ROOT + File.separator + queueIndex, "rw");
                pmemChannels[queueIndex] = rafs[queueIndex].getChannel();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized long allocate(int size){
        if (size < RAM_SPACE_LEVEL_GAP){
            size = RAM_SPACE_LEVEL_GAP;
        }
        // 保证回收后组后一个level的freeQueue有资源回收进去
        if (size > 17 * 1024 - RAM_SPACE_LEVEL_GAP){
            size += RAM_SPACE_LEVEL_GAP - 1;
        }
        if (alreadyAllocateSize + size > PMEM_CACHE_SIZE){
            return 0;
        }
        int retrieveLevelIndex = RamInfo.getRetrieveLevelIndexByDataLen(size);
        long pmemInfo = channelAlreadyAllocateSizes[retrieveLevelIndex] | ((long)(retrieveLevelIndex)<<40);
        if (pmemInfo == 0){
            pmemInfo += 1;
            size += 1;
        }
        channelAlreadyAllocateSizes[retrieveLevelIndex] += size;
        alreadyAllocateSize += size;
        return pmemInfo;
    }
}

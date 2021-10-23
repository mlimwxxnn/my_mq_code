package io.openmessaging.data;

import io.openmessaging.info.PmemInfo;
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

    public synchronized PmemInfo allocate(int size){
        if (size < RAM_SPACE_LEVEL_GAP){
            size = RAM_SPACE_LEVEL_GAP;
        }
        // 保证回收后组后一个level的freeQueue有资源回收进去
        if (size > 17 * 1024 - RAM_SPACE_LEVEL_GAP){
            size += RAM_SPACE_LEVEL_GAP - 1;
        }
        if (alreadyAllocateSize + size > PMEM_CACHE_SIZE){
            return null;
        }
        int retrieveLevelIndex = RamInfo.getRetrieveLevelIndexByDataLen(size);
        PmemInfo pmemInfo = new PmemInfo(channelAlreadyAllocateSizes[retrieveLevelIndex], retrieveLevelIndex);
        channelAlreadyAllocateSizes[retrieveLevelIndex] += size;
        alreadyAllocateSize += size;
        return pmemInfo;
    }
}

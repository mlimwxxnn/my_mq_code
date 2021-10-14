package io.openmessaging.data;

import java.util.concurrent.atomic.AtomicInteger;

public class CacheHitCountData {
    private final AtomicInteger totalQueryCount = new AtomicInteger();
    private final AtomicInteger pmemHitCount = new AtomicInteger();
    private final AtomicInteger ramHitCount = new AtomicInteger();

    public CacheHitCountData(){

    }

    public void addTotalQueryCount(int queryCount) {
        totalQueryCount.getAndAdd(queryCount);
    }

    public void increasePmemHitCount() {
        pmemHitCount.incrementAndGet();
    }

    public void increaseRamHitCount() {
        ramHitCount.incrementAndGet();
    }

    public String getHitCountInfo() {
        int totalCount = totalQueryCount.get();
        int pmemCount = pmemHitCount.get();
        int ramCount = ramHitCount.get();
        double pmemHitPercent = pmemCount * 100.0 / totalCount;
        double ramHitPercent = ramCount * 100.0 / totalCount;
        return String.format("total query count: %d, pmem hit count: %d (%.4f%%), ram hit count: %d (%.4f%%)",
                totalCount, pmemCount, pmemHitPercent, ramCount, ramHitPercent);
    }

    public static void main(String[] args) {
        CacheHitCountData cacheHitCount = new CacheHitCountData();
        cacheHitCount.addTotalQueryCount(3);
        cacheHitCount.increasePmemHitCount();
        cacheHitCount.increasePmemHitCount();
        cacheHitCount.increaseRamHitCount();
        System.out.println(cacheHitCount.getHitCountInfo());
    }
}

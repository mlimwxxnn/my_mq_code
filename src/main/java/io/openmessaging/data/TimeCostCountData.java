package io.openmessaging.data;

import io.openmessaging.DefaultMessageQueueImpl;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TimeCostCountData {
    private String timeCostType;
    private final AtomicInteger ramItemCount = new AtomicInteger();
    private final AtomicInteger pmemItemCount = new AtomicInteger();
    private final AtomicInteger ssdItemCount = new AtomicInteger();
    private final AtomicLong ramCost = new AtomicLong();
    private final AtomicLong pmemCost = new AtomicLong();
    private final AtomicLong ssdCost = new AtomicLong();

    public TimeCostCountData(String timeCostType) {
        this.timeCostType = timeCostType;
        new Thread(() -> {
            try {
                while (true) {
                    // 每隔20s打印一次统计信息
                    Thread.sleep(20 * 1000);
                    DefaultMessageQueueImpl.log.info(getQueryTimeCostInfo());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void addRamTimeCost(long timeCost) {
        ramItemCount.incrementAndGet();
        ramCost.getAndAdd(timeCost);
    }

    public void addPmemTimeCost(long timeCost) {
        pmemItemCount.incrementAndGet();
        pmemCost.getAndAdd(timeCost);
    }

    public void addSsdTimeCost(long timeCost) {
        ssdItemCount.incrementAndGet();
        ssdCost.getAndAdd(timeCost);
    }

    public void addSsdTimeCost(long timeCost, int itemCount) {
        ssdItemCount.addAndGet(itemCount);
        ssdCost.addAndGet(timeCost);
    }

    public String getQueryTimeCostInfo() {
        long ramCostPerData = ramItemCount.get() > 0 ? ramCost.get() / ramItemCount.get() : 0;
        long pmemCostPerData = pmemItemCount.get() > 0 ? pmemCost.get() / pmemItemCount.get() : 0;
        long ssdCostPerData = ssdItemCount.get() > 0 ? ssdCost.get() / ssdItemCount.get() : 0;
        String timeCostInfo = String.format("%s cost per data(ns): {ram: %d, pmem: %d, ssd: %d}, %s times count: {ram: %d, pmem: %d, ssd: %d}",
                timeCostType, ramCostPerData, pmemCostPerData, ssdCostPerData, timeCostType, ramItemCount.get(), pmemItemCount.get(), ssdItemCount.get());
        return timeCostInfo;
    }

    public static void main(String[] args) {
        TimeCostCountData timeCostCountData = new TimeCostCountData("write");
        timeCostCountData.addRamTimeCost(50);
        timeCostCountData.addRamTimeCost(70);
        timeCostCountData.addPmemTimeCost(120);
        timeCostCountData.addPmemTimeCost(130);
        timeCostCountData.addSsdTimeCost(300);
        timeCostCountData.addSsdTimeCost(400);
        timeCostCountData.addSsdTimeCost(500);

        timeCostCountData.addSsdTimeCost(1500, 3);
        System.out.println(timeCostCountData.getQueryTimeCostInfo());
    }
}

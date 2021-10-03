package io.openmessaging;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Test {
    public static Boolean stop = false;
    public static void main(String args[]) throws InterruptedException {
        HashMap<String, Integer> map1 = new HashMap<>();
        ConcurrentHashMap<String, Integer> map2 = new ConcurrentHashMap<>();
        map1.put("wl", 123);
        map2.put("wl1", 123);
        System.out.println(map1.equals(map2));
    }
}

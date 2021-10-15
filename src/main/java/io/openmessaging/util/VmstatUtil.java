package io.openmessaging.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class VmstatUtil {

    static boolean isAccessible = true;
    static final List<SingleItem> SINGLE_ITEMS = new ArrayList<>();

    public static void recordVmstat() {
        run("vmstat 10 700");
    }

    public static void run(String cmd) {
        Thread thread = new Thread(() -> {
            try {
                Process exec = Runtime.getRuntime().exec(cmd);
                InputStream is = exec.getInputStream();
                InputStreamReader rd = new InputStreamReader(is, StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(rd);

                String line = br.readLine();
                for (; ; ) {
                    line = br.readLine();
                    if (line == null) {
                        break;
                    }
                    if (!line.contains("procs")) {
                        synchronized (VmstatUtil.class) {
                            SINGLE_ITEMS.add(SingleItem.build(line));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                isAccessible = false;
            }
        }, "VmstatUtil");
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized static String getVmstatMsg() {
        if (isAccessible) {
            SingleItem singleItem = SINGLE_ITEMS.get(0);
            int[] all = new int[singleItem.values.length];
            for (SingleItem l : SINGLE_ITEMS) {
                for (int i = 0; i < all.length; i++) {
                    if (all[i] < l.values[i].length()) {
                        all[i] = l.values[i].length();
                    }
                }
            }

            for (int i = 0; i < all.length; i++) {
                all[i] += 2;
            }
            StringBuilder stringBuilder = new StringBuilder();
            for (SingleItem l : SINGLE_ITEMS) {
                stringBuilder.append(l.time.toString());
                stringBuilder.append(": ");
                for (int i = 0; i < l.values.length; i++) {
                    stringBuilder.append(String.format("%" + all[i] + "s", l.values[i]));
                }
                stringBuilder.append('\n');
            }
            return stringBuilder.toString();
        } else {
            return "WRONG";
        }
    }

    static class SingleItem {

        LocalDateTime time;
        String[] values;

        static SingleItem build(String line) {
            line = line.trim();
            SingleItem l = new SingleItem();
            l.values = line.split("\\s+");
            l.time = LocalDateTime.now();
            return l;
        }

    }

    public static void main(String[] args) {
        VmstatUtil.recordVmstat();
        try {
            Thread.sleep(3 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(VmstatUtil.getVmstatMsg());
    }

}

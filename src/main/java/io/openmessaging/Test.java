package io.openmessaging;

public class Test {
    public static void main(String[] args) {
        int n = 2 * 10000* 10000;

        long start = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            long res = 1 / ((i % 2) + 1);
        }
        long stop = System.currentTimeMillis();

        long l1 = System.currentTimeMillis();
        for (int i = 1; i < n; i++) {
            try {
                long res = 1 / (i % 2);
            }catch (Exception e){

            }

        }
        long l2 = System.currentTimeMillis();

        System.out.println(l2 - l1);
        System.out.println(stop - start);
    }
}

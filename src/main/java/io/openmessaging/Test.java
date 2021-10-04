package io.openmessaging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Test {

    public static void main(String args[]) throws InterruptedException, IOException {
        File topicFile = new File("for_test", "topic/topic_a");
//
//        if(!topicFile.getParentFile().exists()){
//            topicFile.getParentFile().mkdirs();
//        }
//        if (!topicFile.exists()){
//            topicFile.createNewFile();
//        }
//        FileOutputStream fos = new FileOutputStream(topicFile);
//        fos.write((byte)32);
//        fos.flush();
//        fos.close();
        FileInputStream fis = new FileInputStream(topicFile);
        System.out.println(fis.read());

    }
}

package io.openmessaging.reader;

import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.data.GetRangeTaskData;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DataReader {
    private static final BlockingQueue<GetRangeTaskData> getRangeTaskDataQueue = new LinkedBlockingQueue<>();
    public DataReader(){
        readData();
    }
    public void pushTask(GetRangeTaskData task){
        getRangeTaskDataQueue.offer(task);
    }
    private void readData(){
        for (int i = 0; i < DefaultMessageQueueImpl.READ_THREAD_COUNT; i++) {
            new Thread(()->{
                try {
                    GetRangeTaskData task;
                    while (true) {
                        task = getRangeTaskDataQueue.take();
                        task.queryData();
                        task.getCountDownLatch().countDown();
                    }
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

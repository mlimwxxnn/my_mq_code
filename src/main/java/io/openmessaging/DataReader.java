package io.openmessaging;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DataReader {
    private static final BlockingQueue<GetRangeTask> getRangeTaskQueue = new LinkedBlockingQueue<>();
    public DataReader(){
        readData();
    }
    public void pushTask(GetRangeTask task){
        getRangeTaskQueue.offer(task);
    }
    private void readData(){
        for (int i = 0; i < DefaultMessageQueueImpl.READ_THREAD_COUNT; i++) {
            new Thread(()->{
                try {
                    GetRangeTask task;
                    while (true) {
                        task = getRangeTaskQueue.take();
                        task.queryData();
                        task.countDownLatch.countDown();
                    }
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }).start();
        }
    }
}

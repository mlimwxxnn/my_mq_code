package io.openmessaging.info;


import java.util.LinkedList;

// todo 这里尝试去掉多余的 volatile
public class QueueInfo {
    private int queueLength;
    private final LinkedList<Object> dataPos = new LinkedList<>();

    public Object getDataPosition(){
        return dataPos.pollFirst();
    }
    public void setDataPosition(Object dataPosInfo) {
        queueLength ++;
        dataPos.addLast(dataPosInfo);
    }
    public int size() {
        return queueLength;
    }
}

package io.openmessaging.fordebug;

import java.util.Arrays;

public class QueriedInfo {
    private String topic;
    private int queueId;
    private long offset;
    private int fetchNum;
    private byte[] queryMethod;

    public QueriedInfo(String topic, int queueId, long offset, int fetchNum) {
        this.topic = topic;
        this.queueId = queueId;
        this.offset = offset;
        this.fetchNum = fetchNum;
        this.queryMethod = new byte[100];
    }

    public void setQueryMethod(int i, byte method){
        queryMethod[i] = method;
    }

    @Override
    public String toString() {
        return "QueriedInfo{" +
                "topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", offset=" + offset +
                ", fetchNum=" + fetchNum +
                ", queryMethod=" + Arrays.toString(queryMethod) +
                '}';
    }
}

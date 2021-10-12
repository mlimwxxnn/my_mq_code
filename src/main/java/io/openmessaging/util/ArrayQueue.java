package io.openmessaging.util;

import java.util.AbstractList;

public class ArrayQueue<T> {
    public ArrayQueue(int capacity) {
        this.capacity = capacity + 1;
        this.queue = newArray(capacity + 1);
        this.head = 0;
        this.tail = 0;
        this.size = 0;
    }

    @SuppressWarnings("unchecked")
    private T[] newArray(int size) {
        return (T[]) new Object[size];
    }

    public boolean put(T o) {
        int newtail = (tail + 1) % capacity;
        if (newtail == head)
            return false;
        queue[tail] = o;
        tail = newtail;
        ++size;
        return true;
    }

    public T get() {
        if (head == tail)
            throw new IndexOutOfBoundsException("Queue empty");
        T removed = queue[head];
        queue[head] = null;
        head = (head + 1) % capacity;
        --size;
        return removed;
    }



    public int size() {
        return size;
    }

    public boolean isFull() {
        return capacity - 1 == size;
    }

    public boolean isEmpty(){
        return size == 0;
    }

    private final int capacity;
    private int size;
    private final T[] queue;
    private int head;
    private int tail;
}
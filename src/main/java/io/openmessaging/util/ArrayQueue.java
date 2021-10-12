package io.openmessaging.util;

import java.util.AbstractList;

public class ArrayQueue<T> extends AbstractList<T> {
    public ArrayQueue(int capacity) {
        this.capacity = capacity + 1;
        this.queue = newArray(capacity + 1);
        this.head = 0;
        this.tail = 0;
    }

    @SuppressWarnings("unchecked")
    private T[] newArray(int size) {
        return (T[]) new Object[size];
    }

    public boolean add(T o) {
        int newtail = (tail + 1) % capacity;
        if (newtail == head)
            return false;
        queue[tail] = o;
        tail = newtail;
        return true;
    }

    public T remove(int i) {
        if (i != 0)
            throw new IllegalArgumentException("Can only remove head of queue");
        if (head == tail)
            throw new IndexOutOfBoundsException("Queue empty");
        T removed = queue[head];
        queue[head] = null;
        head = (head + 1) % capacity;
        return removed;
    }



    public int size() {
        // Can't use % here because it's not mod: -3 % 2 is -1, not +1.
        int diff = tail - head;
        if (diff < 0)
            diff += capacity;
        return diff;
    }

    private int capacity;
    private T[] queue;
    private int head;
    private int tail;
}
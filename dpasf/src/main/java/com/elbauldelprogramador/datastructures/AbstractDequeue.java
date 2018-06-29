package com.elbauldelprogramador.datastructures;

public abstract class AbstractDequeue<E> extends AbstractQueue<E> {
    @Override
    public E peek() {
        return peekFirst();
    }

    abstract public E peekLast();

    abstract public E peekFirst();

    @Override
    public E poll() {
        return pollFirst();
    }

    abstract public E pollLast();

    abstract public E pollFirst();
}

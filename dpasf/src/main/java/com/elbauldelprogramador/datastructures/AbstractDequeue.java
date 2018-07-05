package com.elbauldelprogramador.datastructures;

abstract class AbstractDequeue<E> extends AbstractQueue<E> {
    @Override
    public E peek() {
        return peekFirst();
    }

    abstract public E peekLast();

    protected abstract E peekFirst();

    @Override
    public E poll() {
        return pollFirst();
    }

    abstract public E pollLast();

    protected abstract E pollFirst();
}

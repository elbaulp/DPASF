package com.elbauldelprogramador.datastructures;

abstract class AbstractDequeue<E> extends AbstractQueue<E> {
    abstract public E peekLast();

    @Override
    public E poll() {
        return pollFirst();
    }

    @Override
    public E peek() {
        return peekFirst();
    }

    protected abstract E peekFirst();

    protected abstract E pollFirst();

    abstract public E pollLast();
}

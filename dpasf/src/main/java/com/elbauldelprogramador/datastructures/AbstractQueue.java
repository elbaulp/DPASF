package com.elbauldelprogramador.datastructures;

public abstract class AbstractQueue<E> extends java.util.AbstractQueue<E> {
    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object e) {
        return removeElem((E) e);
    }

    abstract public boolean removeElem(E e);
}

package com.elbauldelprogramador.datastructures;

abstract class AbstractQueue<E> extends java.util.AbstractQueue<E> {
    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object e) {
        return removeElem((E) e);
    }

    protected abstract boolean removeElem(E e);
}

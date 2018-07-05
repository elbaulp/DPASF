package com.elbauldelprogramador.datastructures;

import java.io.Serializable;
import java.util.Comparator;

class NaturalComparator<E> implements Comparator<E>, Serializable {
    @Override
    @SuppressWarnings("unchecked")
    public int compare(E o1, E o2) {
        return ((Comparable<E>) o1).compareTo(o2);
    }
}

package com.elbauldelprogramador.datastructures;

import java.util.Comparator;
import java.io.Serializable;

public class NaturalComparator<E> implements Comparator<E>, Serializable {
    @Override
    @SuppressWarnings("unchecked")
    public int compare(E o1, E o2) {
        return ((Comparable<E>) o1).compareTo(o2);
    }
}

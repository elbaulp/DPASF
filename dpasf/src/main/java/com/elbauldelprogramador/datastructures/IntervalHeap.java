package com.elbauldelprogramador.datastructures;

import java.io.Serializable;
import java.util.*;

import static java.lang.Math.log;
import static java.util.Collections.swap;

/**
 * Double ended priority queue implemented as an interval heap.
 * <p>
 * This collection provides efficient access to the minimum and maximum elements
 * that it contains. The minimum and maximum elements can be queried in constant
 * O(1) time, and removed in O(log(N)) time. Elements can be added in O(log(N))
 * time.
 * <p>
 * A textbook implementation will describe an array of intervals, where each
 * interval has a large and small value. This implementation takes care to avoid
 * allocating many small objects to record the intervals. Instead, this
 * implementation maintains a min heap in the even array indices, and a max heap
 * in the odd array indices. Intervals can be reconstructed from the even odd
 * pairs.
 *
 * Taken from https://github.com/allenbh/gkutil_java/blob/master/src/gkimfl/util/IntervalHeap.java
 *
 * @param <E> - the type of elements held in this collection
 * @author Allen Hubbe
 */
public class IntervalHeap<E> extends AbstractDequeue<E> implements Serializable {
    private final Comparator<E> cmp;
    private final List<E> queue;


    public IntervalHeap() {
        cmp = new NaturalComparator<E>();
        queue = new ArrayList<E>();
    }

    public IntervalHeap(IntervalHeap<E> other) {
        cmp = other.cmp;
        queue = new ArrayList<E>(other.queue);
    }

    public IntervalHeap(Comparator<E> comparator) {
        cmp = comparator;
        queue = new ArrayList<E>();
    }

    public IntervalHeap(Collection<? extends E> c) {
        cmp = new NaturalComparator<E>();
        queue = new ArrayList<E>(c);
        heapify();
    }

    public IntervalHeap(Collection<? extends E> c, Comparator<E> comparator) {
        cmp = comparator;
        queue = new ArrayList<E>(c);
        heapify();
    }

    public IntervalHeap(int initialCapacity) {
        cmp = new NaturalComparator<E>();
        queue = new ArrayList<E>(initialCapacity);
    }

    public IntervalHeap(int initialCapacity, Comparator<E> comparator) {
        cmp = comparator;
        queue = new ArrayList<E>(initialCapacity);
    }

    /**
     * Remove all elements from the heap.
     */
    @Override
    public void clear() {
        queue.clear();
    }

    /**
     * Return true if the heap is empty.
     */
    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * Return an iterator for the elements. This iterator does not yield
     * elements in sorted order.
     */
    @Override
    public Iterator<E> iterator() {
        return queue.iterator();
    }

    /**
     * Insert several elements into the heap. If the number of elements to be
     * added is large, this may call heapify for efficiency instead of adding
     * the elements one at a time.
     */
    @Override
    public boolean addAll(Collection<? extends E> c) {
        int cSize = c.size();
        int nSize = cSize + queue.size();
        if (nSize <= cSize * log(nSize) / log(2)) {
            queue.addAll(c);
            heapify();
            return true;
        } else {
            return super.addAll(c);
        }
    }

    /**
     * Insert an element into the heap.
     */
    @Override
    public boolean offer(E e) {
        queue.add(e);
        int iBound = queue.size();
        int i = iBound - 1;
        if ((i & 1) == 0) {
            pullUpMax(i);
            pullUpMin(i);
        } else {
            pullUpMax(i);
            if (lessAt(i, i - 1)) {
                swap(queue, i, i - 1);
                pullUpMin(i - 1);
                pullUpMax(i);
            }
        }
        return true;
    }

    /**
     * Return the minimum element.
     */
    @Override
    public E peekFirst() {
        return queue.get(0);
    }

    /**
     * Return the maximum element.
     */
    @Override
    public E peekLast() {
        if (queue.size() < 2) {
            return queue.get(0);
        }
        return queue.get(1);
    }

    /**
     * Return and remove the minimum element.
     */
    @Override
    public E pollFirst() {
        int iBound = queue.size() - 1;
        if (iBound < 1) {
            return queue.remove(0);
        } else {
            E e = queue.get(0);
            if (iBound > 0) {
                queue.set(0, queue.remove(iBound));
                int i = pushDownMin(0);
                if (i + 1 == iBound) {
                    pullUpMax(i);
                } else if (i + 1 < iBound && lessAt(i + 1, i)) {
                    // i is a leaf of the min heap
                    assert ((i << 1) + 2 > iBound);
                    swap(queue, i + 1, i);
                    pullUpMax(i + 1);
                }
            }
            return e;
        }
    }

    /**
     * Return and remove the maximum element.
     */
    @Override
    public E pollLast() {
        int iBound = queue.size() - 1;
        if (iBound < 1) {
            return queue.remove(0);
        } else {
            E e = queue.get(1);
            if (iBound < 2) {
                queue.remove(1);
            } else {
                queue.set(1, queue.remove(iBound));
                int i = pushDownMax(1);
                if ((i & 1) == 0) {
                    assert (i + 1 == iBound);
                    pullUpMin(i);
                } else if (lessAt(i, i - 1)) {
                    // i is a leaf of the max heap
                    assert ((i << 1) + 1 > iBound);
                    swap(queue, i, i - 1);
                    pullUpMin(i - 1);
                }
            }
            return e;
        }
    }

    /**
     * Removing arbitrary elements is not supported.
     */
    @Override
    public boolean removeElem(E e) {
        throw new UnsupportedOperationException();
    }

    /**
     * Return the number of elements in the heap.
     */
    @Override
    public int size() {
        return queue.size();
    }

    /**
     * Return true if vA should should be ordered prior to vB.
     */
    private boolean less(E vA, E vB) {
        return cmp.compare(vA, vB) < 0;
    }

    /**
     * Return true if the value at iA should should be ordered prior to the
     * value at iB.
     */
    private boolean lessAt(int iA, int iB) {
        return less(queue.get(iA), queue.get(iB));
    }

    /**
     * Efficiently order elements into heap. This operation is guaranteed to run
     * in O(N) time, N being the number of elements. Like a normal
     * (non-interval) heap, elements are ordered starting from the leaves, and
     * pushed down towards the leaves. Unlike a normal heap, elements must be
     * pulled back up from the leaves. The pull up operation is bounded by the
     * current position of progress of the heapify algorithm, to ensure
     * correctness and guarantee efficiency.
     */
    private void heapify() {
        int iBound = queue.size();
        for (int i = iBound - 1; 0 <= i; --i) {
            if ((i & 1) == 0) {
                int j = pushDownMin(i);
                if (j + 1 == iBound) {
                    pullUpMax(j, i + 1);
                } else if (j + 1 < iBound && lessAt(j + 1, j)) {
                    swap(queue, j + 1, j);
                    pullUpMin(j, i);
                    pullUpMax(j + 1, i + 1);
                }
            } else {
                if (0 <= i - 1 && lessAt(i, i - 1)) {
                    swap(queue, i, i - 1);
                }

                int j = pushDownMax(i);
                if ((j & 1) == 0) {
                    assert (i < j);
                    assert (j + 1 == iBound);
                    pullUpMin(j, i + 1);
                } else if (i < j && lessAt(j, j - 1)) {
                    swap(queue, j, j - 1);
                    pullUpMax(j, i);
                    pullUpMin(j - 1, i + 1);
                }
            }
        }
    }

    /**
     * Pull an element at position i up in the max heap until it satisfies the
     * max heap invariant. The initial position i normally corresponds to a leaf
     * in the max heap, but it may be a leaf in the min heap in case of a leaf
     * representing an empty interval.
     */
    private int pullUpMax(int i) {
        E v = queue.get(i);
        while (1 < i) {
            int iUp = ((i >> 1) - 1) | 1;
            E vUp = queue.get(iUp);
            if (!less(vUp, v)) {
                break;
            }
            queue.set(i, vUp);
            i = iUp;
        }
        queue.set(i, v);
        return i;
    }

    /**
     * Pull an element at position i up in the min heap until it satisfies the
     * min heap invariant. The initial position i should always be in the min
     * heap.
     */
    private int pullUpMin(int i) {
        E v = queue.get(i);
        while (0 < i) {
            int iUp = ((i >> 1) - 1) & ~1;
            E vUp = queue.get(iUp);
            if (!less(v, vUp)) {
                break;
            }
            queue.set(i, vUp);
            i = iUp;
        }
        queue.set(i, v);
        return i;
    }

    /**
     * Pull an element at position i up in the max heap until it satisfies the
     * max heap invariant, but do not consider ancestors before position base.
     */
    private int pullUpMax(int i, int base) {
        E v = queue.get(i);
        while (base < i) {
            int iUp = ((i >> 1) - 1) | 1;
            E vUp = queue.get(iUp);
            if (iUp < base || !less(vUp, v)) {
                break;
            }
            queue.set(i, vUp);
            i = iUp;
        }
        queue.set(i, v);
        return i;
    }

    /**
     * Pull an element at position i up in the min heap until it satisfies the
     * min heap invariant, but do not consider ancestors before position base.
     */
    private int pullUpMin(int i, int base) {
        E v = queue.get(i);
        while (base < i) {
            int iUp = ((i >> 1) - 1) & ~1;
            E vUp = queue.get(iUp);
            if (iUp < base || !less(v, vUp)) {
                break;
            }
            queue.set(i, vUp);
            i = iUp;
        }
        queue.set(i, v);
        return i;
    }

    /**
     * Push an element at position i down in the max heap until it satisfies the
     * max heap invariant. The resulting position is normally in the max heap,
     * but may be in the min heap if it is a leaf representing an empty
     * interval.
     */
    private int pushDownMax(int i) {
        int iBound = queue.size();
        E v = queue.get(i);
        while (true) {
            int iDown = (i << 1) + 1;
            E vDown;
            if (iBound < iDown) {
                break;
            }
            if (iDown == iBound) {
                iDown = iBound - 1;
                vDown = queue.get(iDown);
            } else {
                vDown = queue.get(iDown);
                int iRight = iDown + 2;
                if (iRight <= iBound) {
                    if (iRight == iBound) {
                        iRight = iBound - 1;
                    }
                    E vRight = queue.get(iRight);
                    if (less(vDown, vRight)) {
                        vDown = vRight;
                        iDown = iRight;
                    }
                }
            }
            if (!less(v, vDown)) {
                break;
            }
            queue.set(i, vDown);
            i = iDown;
        }
        queue.set(i, v);
        return i;
    }

    /**
     * Push an element at position i down in the min heap until it satisfies the
     * min heap invariant. The resulting position will be in the min heap.
     */
    private int pushDownMin(int i) {
        int iBound = queue.size();
        E v = queue.get(i);
        while (true) {
            int iDown = (i << 1) + 2;
            if (iBound <= iDown) {
                break;
            }
            E vDown = queue.get(iDown);
            int iRight = iDown + 2;
            if (iRight < iBound) {
                E vRight = queue.get(iRight);
                if (less(vRight, vDown)) {
                    vDown = vRight;
                    iDown = iRight;
                }
            }
            if (!less(vDown, v)) {
                break;
            }
            queue.set(i, vDown);
            i = iDown;
        }
        queue.set(i, v);
        return i;
    }
}

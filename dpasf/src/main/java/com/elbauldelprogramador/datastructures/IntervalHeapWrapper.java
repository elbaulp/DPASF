/*
 ** Class for a discretisation filter for instance streams
 ** Copyright (C) 2016 Germain Forestier, Geoffrey I Webb, Sergio Ramírez
 **
 ** This program is free software: you can redistribute it and/or modify
 ** it under the terms of the GNU General Public License as published by
 ** the Free Software Foundation, either version 3 of the License, or
 ** (at your option) any later version.
 **
 ** This program is distributed in the hope that it will be useful,
 ** but WITHOUT ANY WARRANTY; without even the implied warranty of
 ** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 ** GNU General Public License for more details.
 **
 ** You should have received a copy of the GNU General Public License
 ** along with this program. If not, see <http://www.gnu.org/licenses/>.
 **
 ** Please report any bugs to Sergio Ramírez <sramirez@decsai.ugr.es> and
 ** Germain Forestier <germain.forestier@uha.fr>
 */
package com.elbauldelprogramador.datastructures;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;

/**
 * A IntervalHeapWrapper storing the Discretization. Indexed by attribute
 *
 * Each IntervalHeap V, $V_i^j$ stores the values for the j-th bin
 * of $X_i$
 *
 * An adaptation of https://github.com/sramirez/MOAReduction/blob/master/src/moa/reduction/bayes/IntervalHeap.java
 *
 * @param bins Number of bins to use for discretize
 * @param attrIndex Index of the attribute
 * @param s sample size
 */
public class IntervalHeapWrapper implements Serializable {

    // index of this attribute
    protected int attIndex;
    // number of bins
    protected int nBins;
    // number of samples seen in this discretization
    protected int nbSamples;
    // number of samples (i.e. size of the window)
    protected int sampleSize;
    // double priority queues
    protected IntervalHeap<Double>[] values;
    // the value of the current window (might not be used if random sample)
    protected LinkedList<Double> windowValues = new LinkedList<Double>();

    public IntervalHeapWrapper(int nBins, int sampleSize, int attIndex) {
        this.nBins = nBins;
        this.sampleSize = sampleSize;
        values = new IntervalHeap[nBins];
        for (int i = 0; i < nBins; i++) {
            values[i] = new IntervalHeap<>();
        }
        this.attIndex = attIndex;
    }

    public int getNbSamples() {
        return nbSamples;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("Attribute [" + attIndex + "] \t");
        for (int i = 0; i < values.length; i++) {
            if (!values[i].isEmpty()) {
                buffer.append(
                        "[" + values[i].peekFirst() + ";" + values[i].peekLast() + "](" + values[i].size() + ") ");
                // buffer.append("\t\t"+values[i]+"\n");

            } else {
                buffer.append("[;]");
            }
        }
        // buffer.append("\n"+windowValues);
        return buffer.toString();
    }

    public double[] getBoundaries() {

        LinkedHashSet<Double> uniquePoints = new LinkedHashSet<Double>();
        for (int i = 0; i < values.length; i++) {
            if (!values[i].isEmpty()) {
                uniquePoints.add(values[i].peekLast());
            }
        }
        /* Return only unique points, in order to accomplish conditions
         * marked by the discretizer template */
        int j = 0;
        double[] bounds = new double[uniquePoints.size()];
        for (Iterator<Double> iterator = uniquePoints.iterator(); iterator
                .hasNext(); ) {
            bounds[j] = (double) iterator.next();

            j++;
        }
        return bounds;
    }

    /**
     * Return the bin for this value
     *
     * @param v the value to find
     * @return the bin
     */
    public int getBin(double v) {
        int cv = 0;

        while (cv < nBins && !values[cv].isEmpty() && v > values[cv].peekLast()) {
            cv++;
        }

        // if the value spans the entire next bin then return the next bin
        if (cv < nBins - 1 && !values[cv + 1].isEmpty() && v == values[cv + 1].peekLast()) {
            cv++;
        }

        return cv;
    }

    private void replaceValue(double r, double v) {
        double oldV = r;
        double newV = v;

        if (oldV == newV)
            return;

        int oldBin = 0; /// < the bin containing the old value
        int newBin = 0; /// < the bin to contain new value

        // advance until the bin contains the value
        while (oldBin < nBins - 1 && !values[oldBin + 1].isEmpty() && oldV >= values[oldBin + 1].peekFirst()) {
            oldBin++;
        }

        // remove the value
        values[oldBin].remove(oldV);

        // advance while v can't go into this bin
        while (newBin < nBins - 1 && !values[newBin + 1].isEmpty() && newV > values[newBin + 1].peekFirst()) {
            newBin++;
        }

        while (newBin < oldBin && newV >= values[newBin].peekLast()) {
            // v falls between intervals so insert into the one closer to the target
            newBin++;
        }

        int loc = newBin;

        if (oldBin >= newBin) {
            // need to shuffle replaced value up
            while (loc < oldBin) {
                double valToMove = values[loc].pollLast();
                values[loc + 1].offer(valToMove);
                loc++;
            }
        } else {
            // need to shuffle replaced value down
            while (loc > oldBin) {
                double valToMove = values[loc].pollFirst();
                values[loc - 1].offer(valToMove);
                loc--;
            }
        }

        values[newBin].offer(newV);
        nbSamples++;

        checkOrder();
        checkSize();
    }

    public void insertWithWindow(double v) {
        if (windowValues.size() < sampleSize) {
            // the sample is not full so need to add the value to the queue and the sample
            windowValues.offer(v);
            insertValue(v);
        } else {
            // the sample is full so need to replace the oldest value with this one
            double r = windowValues.get(0);
            windowValues.remove(0);

            // if values are different, update
            if (r != v) {
                replaceValue(r, v);
            }

            // add the new value at the end of ordered list
            windowValues.offer(v);
        }
    }

    public void insertValue(double v) {
        int targetbin = nbSamples % nBins; /// < the bin needing to expand
        int loc = 0; /// < the bin into which this value goes

        // advance while v can't go into this bin
        while (loc < nBins - 1 && !values[loc + 1].isEmpty() && v > values[loc + 1].peekFirst()) {
            loc++;
        }

        // no bin before targetbin can be empty
        while (loc < targetbin && v >= values[loc].peekLast()) {
            // v falls between intervals so insert into the one closer to the
            // target
            loc++;
        }

        int insertLoc = loc;

        if (targetbin >= loc) {
            // need to shuffle replaced value up
            while (loc < targetbin) {
                double valToMove = values[targetbin - 1].pollLast();
                values[targetbin].offer(valToMove);
                targetbin--;
            }
        } else {
            // need to shuffle replaced value down
            while (loc > targetbin) {
                double valToMove = values[targetbin + 1].pollFirst();
                values[targetbin].offer(valToMove);
                targetbin++;
            }
        }
        values[insertLoc].offer(v);
        nbSamples++;

        checkOrder();
        checkSize();
    }

    /**
     * Replace the ith value by v
     *
     * @param index the index of the value to remove
     * @param v     the value to add
     */
    public void replace(int index, double v) {
        int replacementBin = 0;

        // find in which bin "index" is located
        while (index >= values[replacementBin].size()) {
            index -= values[replacementBin].size();
            replacementBin++;
        }

        // find the value to replace by iterating on the bin
        double vToReplace = 0.0;
        int count = 0;
        for (Double it : values[replacementBin]) {
            if (count == index) {
                vToReplace = it;
                break;
            }
            count++;
        }

        // replace the found value by v
        replaceValue(vToReplace, v);
    }

    public boolean checkValueInQueues(double v) {
        boolean res = false;
        for (int i = 0; i < values.length; i++) {
            res = res || values[i].contains(v);
        }
        return res;
    }

    private void checkSize() {
        for (int i = 0; i < values.length - 1; i++) {
            if ((!values[i].isEmpty() && !values[i + 1].isEmpty()) && values[i].size() < values[i + 1].size()) {
                System.out.println("wrong size");
                System.out.println(this.toString());
                System.out.println();
            }
        }

    }

    public void checkOrder() {
        for (int i = 0; i < values.length - 1; i++) {
            if ((!values[i].isEmpty() && !values[i + 1].isEmpty()) && values[i].peekLast() > values[i + 1].peekFirst()) {
                System.out.println("wrong order");
                System.out.println();
            }
        }
    }
}



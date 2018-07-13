//package com.elbauldelprogramador.featureselection;
//
///*
// *   This program is free software: you can redistribute it and/or modify
// *   it under the terms of the GNU General Public License as published by
// *   the Free Software Foundation, either version 3 of the License, or
// *   (at your option) any later version.
// *
// *   This program is distributed in the hope that it will be useful,
// *   but WITHOUT ANY WARRANTY; without even the implied warranty of
// *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// *   GNU General Public License for more details.
// *
// *   You should have received a copy of the GNU General Public License
// *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//
///*
// *    InfoGainAttributeEval.java
// *    Copyright (C) 1999-2012 University of Waikato, Hamilton, New Zealand
// *
// */
//
//
//import com.elbauldelprogramador.featureselection.InfoGainTransformer.Key;
//import weka.core.ContingencyTables;
//import weka.filters.supervised.attribute.Discretize;
//import weka.filters.unsupervised.attribute.NumericToBinary;
//
//import java.io.Serializable;
//import java.util.*;
//import java.util.Map.Entry;
//
///**
// * <!-- globalinfo-start --> InfoGainAttributeEval :<br/>
// * <br/>
// * Evaluates the worth of an attribute by measuring the information gain with
// * respect to the class.<br/>
// * <br/>
// * InfoGain(Class,Attribute) = H(Class) - H(Class | Attribute).<br/>
// * <p/>
// * <!-- globalinfo-end -->
// * <p>
// * <!-- options-start --> Valid options are:
// * <p/>
// * <p>
// * <pre>
// * -M
// *  treat missing values as a seperate value.
// * </pre>
// * <p>
// * <pre>
// * -B
// *  just binarize numeric attributes instead
// *  of properly discretizing them.
// * </pre>
// * <p>
// * <!-- options-end -->
// *
// * @author Mark Hall (mhall@cs.waikato.ac.nz)
// * @version $Revision: 10172 $
// * @see Discretize
// * @see NumericToBinary
// */
//public class IncrementalInfoGain implements Serializable {
//
//
//    /**
//     * Update the contingency tables and the rankings for each features using the counters.
//     * Counters are updated in each iteration.
//     */
//    public static Double applySelection(Map<Key, Double> counts) {
//        Set<Key> keys = counts.keySet();
//        Set<Entry<Key, Double>> entries = counts.entrySet();
//
//        Set<Double> avalues = new HashSet<>();
//        Set<Double> cvalues = new HashSet<>();
//
//        for (Key key : keys) {
//            avalues.add(key.x());
//            cvalues.add(key.y());
//        }
//
//        Map<Double, Integer> apos = new HashMap<>();
//        Map<Double, Integer> cpos = new HashMap<>();
//
//        int aidx = 0;
//        for (Double f : avalues) {
//            apos.put(f, aidx++);
//        }
//
//        int cidx = 0;
//        for (Double f : cvalues) {
//            cpos.put(f, cidx++);
//        }
//
//        double[][] lcounts = new double[avalues.size()][cvalues.size()];
//        for (Entry<Key, Double> entry : entries) {
//            lcounts[apos.get(entry.getKey().x())][cpos.get(entry.getKey().y())] = entry.getValue();
//        }
//
////        switch (2) {
////            case 1:
////                m_InfoValues = ContingencyTables.symmetricalUncertainty(lcounts);
////                break;
////
////            default:
////                m_InfoValues = (ContingencyTables.entropyOverColumns(lcounts) - ContingencyTables
////                        .entropyConditionedOnRows(lcounts));
////                break;
////        }
////        return m_InfoValues;
//        return (ContingencyTables.entropyOverColumns(lcounts) - ContingencyTables
//                .entropyConditionedOnRows(lcounts));
//    }
//
////    public Double evaluateAttribute(int attribute) {
////        return m_InfoValues;
////    }
//}
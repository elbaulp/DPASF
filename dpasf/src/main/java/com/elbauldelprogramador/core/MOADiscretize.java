package com.elbauldelprogramador.core;

import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import weka.core.Range;
import weka.filters.Filter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


public abstract class MOADiscretize
        extends Filter
        implements Serializable {
    public boolean m_Init = false;
    public boolean provideProb = false;
    /**
     * Stores which columns to Discretize
     */
    protected Range m_DiscretizeCols = new Range();
    /**
     * Store the current cutpoints
     */
    protected double[][] m_CutPoints = null;
    protected String[][] m_Labels = null;
    /**
     * Precision for bin range labels
     */
    protected int m_BinRangePrecision = 6;

    /**
     * Sets the format of the input instances.
     *
     * @param instanceInfo an Instances object containing the input instance
     *                     structure (any instances contained in the object are ignored -
     *                     only the structure is required).
     * @return true if the outputFormat may be collected immediately
     * @throws Exception if the input format can't be set successfully
     */
    @Override
    public boolean setInputFormat(weka.core.Instances instanceInfo) throws Exception {

        super.setInputFormat(instanceInfo);

        m_DiscretizeCols.setUpper(instanceInfo.numAttributes() - 1);
        m_CutPoints = null;

        // If we implement loading cutfiles, then load
        // them here and set the output format
        return false;
    }

    /**
     * Sets which attributes are to be Discretized (only numeric attributes among
     * the selection will be Discretized).
     *
     * @param rangeList a string representing the list of attributes. Since the
     *                  string will typically come from a user, attributes are indexed
     *                  from 1. <br>
     *                  eg: first-3,5,6-last
     * @throws IllegalArgumentException if an invalid range list is supplied
     */
    public void setAttributeIndices(String rangeList) {

        m_DiscretizeCols.setRanges(rangeList);
    }

    /**
     * Gets the cut points for an attribute
     *
     * @param attributeIndex the index (from 0) of the attribute to get the cut
     *                       points of
     * @return an array containing the cutpoints (or null if the attribute
     * requested isn't being Discretized
     */
    public double[] getCutPoints(int attributeIndex) {

        if (m_CutPoints == null) {
            return null;
        }
        return m_CutPoints[attributeIndex];
    }

    /**
     * Set the output format. Takes the currently defined cutpoints and
     * m_InputFormat and calls setOutputFormat(Instances) appropriately.
     */
    protected LabeledVector changeOutputFormat(LabeledVector inst) {

        if (m_CutPoints == null) {
            //setOutputFormat(null);
            return null;
        }
        int nAttrs = inst.vector().size();
        ArrayList<Double> attributes = new ArrayList<>(nAttrs);
        DenseVector dense = DenseVector.zeros(nAttrs);
        for (int i = 0, m = nAttrs; i < m; ++i) {
            if ((m_DiscretizeCols.isInRange(i))) {

                Set<String> cutPointsCheck = new HashSet<>();
                double[] cutPoints = m_CutPoints[i];

                double attrvalue = 0.0;
                if (cutPoints != null) {
                    boolean predefinedLabels = false;
                    if (m_Labels != null && m_Labels[i] != null) {
                        if (m_Labels[i].length == m_CutPoints[i].length)
                            predefinedLabels = true;
                    }
                    if (predefinedLabels) {
                        for (int j = 0; j < m_Labels[i].length; j++) {
                            attrvalue = Double.parseDouble(m_Labels[i][j]);
                        }
                    }
                }
                dense.update(i, attrvalue);
            }
        }
        return new LabeledVector(inst.label(), dense);
    }


    /**
     * Convert a single instance over. The converted instance is added to the end
     * of the output queue.
     *
     * @param instance the instance to convert
     */
    protected LabeledVector convertInstance(LabeledVector instance) {
        int index = 0;
        int numAttributes = instance.vector().size();
        m_DiscretizeCols.setUpper(instance.vector().size()); // Important (class is removed from discretization)
        double[] vals = new double[numAttributes]; // +1 for the class
        // Copy and convert the values
        for (int i = 0; i < numAttributes; i++) {
            if (m_DiscretizeCols.isInRange(i)) {
                int j;
                float currentVal = (float) instance.vector().apply(i);
                if (m_CutPoints[i] == null) {
                    vals[index] = 0;
                } else {
                    for (j = 0; j < m_CutPoints[i].length; j++) {
                        float cp = (float) (Math.round(m_CutPoints[i][j] * 1000000.0) / 1000000.0);
                        if (currentVal <= cp) {
                            break;
                        }
                    }
                    if (m_Labels != null) {
                        if (m_Labels[i] != null) {
                            if (j < m_Labels[i].length)
                                vals[index] = Float.parseFloat(m_Labels[i][j]);
                            else
                                vals[index] = 0;
                        } else {
                            vals[index] = j;
                        }
                    } else {
                        vals[index] = j;
                    }
                }
            } else {
                vals[index] = instance.vector().apply(i);
            }
            index++;
        }

        return new LabeledVector(instance.label(), new DenseVector(vals));
    }

    protected void writeCPointsToFile(int att1, int att2, int iteration, String method) {
        FileWriter cpoints1 = null;
        FileWriter cpoints2 = null;
        try {
            cpoints1 = new FileWriter(method + "-cpoints1" + "-" + iteration + ".dat");
            cpoints2 = new FileWriter(method + "-cpoints2" + "-" + iteration + ".dat");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        PrintWriter cpout1 = new PrintWriter(Objects.requireNonNull(cpoints1));
        PrintWriter cpout2 = new PrintWriter(Objects.requireNonNull(cpoints2));

        if (m_CutPoints != null && m_CutPoints[att1] != null) {
            for (int i = 0; i < m_CutPoints[att1].length; i++) {
                cpout1.println(m_CutPoints[att1][i]);
            }
        }

        if (m_CutPoints != null && m_CutPoints[att2] != null) {
            for (int i = 0; i < m_CutPoints[att2].length; i++) {
                cpout2.println(m_CutPoints[att2][i]);
            }
        }
        //Flush the output to the file;
        cpout1.flush();
        cpout2.flush();

        //Close the Print Writer
        cpout1.close();
        cpout2.close();

        //Close the File Writer
        try {
            cpoints1.close();
            cpoints2.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public abstract Float condProbGivenClass(int attI, double rVal, int dVal, int classVal, float classProb);

    public abstract Float condProbGivenClass(int attI, double rVal, int dVal, int classVal, int classCount);

    public abstract int getAttValGivenClass(int attI, double rVal, int dVal, int classVal);

    public abstract void updateEvaluator(LabeledVector inst);

    public abstract LabeledVector applyDiscretization(LabeledVector inst);

}
package com.elbauldelprogramador.core;

import com.yahoo.labs.samoa.instances.*;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import weka.core.Attribute;
import weka.core.ContingencyTables;
import weka.core.Range;
import weka.core.Utils;
import weka.filters.Filter;

import java.awt.*;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


public abstract class MOADiscretize
        extends Filter
        implements Serializable {
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
     * Output binary attributes for discretized attributes.
     */
    protected boolean m_MakeBinary = false;

    /**
     * Use bin numbers rather than ranges for discretized attributes.
     */
    protected boolean m_UseBinNumbers = false;

    /**
     * Use better encoding of split point for MDL.
     */
    protected boolean m_UseBetterEncoding = false;

    /**
     * Use Kononenko's MDL criterion instead of Fayyad et al.'s
     */
    protected boolean m_UseKononenko = false;

    /**
     * Precision for bin range labels
     */
    protected int m_BinRangePrecision = 6;

    public boolean m_Init = false;

    public boolean provideProb = false;

    /**
     * Constructor - initialises the filter
     */
    public MOADiscretize() {
        setAttributeIndices("first-last");
    }


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
     * Returns the tip text for this property
     *
     * @return tip text for this property suitable for displaying in the
     * explorer/experimenter gui
     */
    public String binRangePrecisionTipText() {
        return "The number of decimal places for cut points to use when generating bin labels";
    }

    /**
     * Set the precision for bin boundaries. Only affects the boundary values used
     * in the labels for the converted attributes; internal cutpoints are at full
     * double precision.
     *
     * @param p the precision for bin boundaries
     */
    public void setBinRangePrecision(int p) {
        m_BinRangePrecision = p;
    }

    /**
     * Get the precision for bin boundaries. Only affects the boundary values used
     * in the labels for the converted attributes; internal cutpoints are at full
     * double precision.
     *
     * @return the precision for bin boundaries
     */
    public int getBinRangePrecision() {
        return m_BinRangePrecision;
    }

    /**
     * Returns the tip text for this property
     *
     * @return tip text for this property suitable for displaying in the
     * explorer/experimenter gui
     */
    public String makeBinaryTipText() {

        return "Make resulting attributes binary.";
    }

    /**
     * Gets whether binary attributes should be made for discretized ones.
     *
     * @return true if attributes will be binarized
     */
    public boolean getMakeBinary() {

        return m_MakeBinary;
    }

    /**
     * Sets whether binary attributes should be made for discretized ones.
     *
     * @param makeBinary if binary attributes are to be made
     */
    public void setMakeBinary(boolean makeBinary) {

        m_MakeBinary = makeBinary;
    }

    /**
     * Returns the tip text for this property
     *
     * @return tip text for this property suitable for displaying in the
     * explorer/experimenter gui
     */
    public String useBinNumbersTipText() {
        return "Use bin numbers (eg BXofY) rather than ranges for for discretized attributes";
    }

    /**
     * Gets whether bin numbers rather than ranges should be used for discretized
     * attributes.
     *
     * @return true if bin numbers should be used
     */
    public boolean getUseBinNumbers() {

        return m_UseBinNumbers;
    }

    /**
     * Sets whether bin numbers rather than ranges should be used for discretized
     * attributes.
     *
     * @param useBinNumbers if bin numbers should be used
     */
    public void setUseBinNumbers(boolean useBinNumbers) {

        m_UseBinNumbers = useBinNumbers;
    }

    /**
     * Gets whether better encoding is to be used for MDL.
     *
     * @return true if the better MDL encoding will be used
     */
    public boolean getUseBetterEncoding() {

        return m_UseBetterEncoding;
    }

    /**
     * Sets whether better encoding is to be used for MDL.
     *
     * @param useBetterEncoding true if better encoding to be used.
     */
    public void setUseBetterEncoding(boolean useBetterEncoding) {

        m_UseBetterEncoding = useBetterEncoding;
    }

    /**
     * Gets whether the supplied columns are to be removed or kept
     *
     * @return true if the supplied columns will be kept
     */
    public boolean getInvertSelection() {

        return m_DiscretizeCols.getInvert();
    }

    /**
     * Sets whether selected columns should be removed or kept. If true the
     * selected columns are kept and unselected columns are deleted. If false
     * selected columns are deleted and unselected columns are kept.
     *
     * @param invert the new invert setting
     */
    public void setInvertSelection(boolean invert) {

        m_DiscretizeCols.setInvert(invert);
    }

    /**
     * Gets the current range selection
     *
     * @return a string containing a comma separated list of ranges
     */
    public String getAttributeIndices() {

        return m_DiscretizeCols.getRanges();
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
     * Sets which attributes are to be Discretized (only numeric attributes among
     * the selection will be Discretized).
     *
     * @param attributes an array containing indexes of attributes to Discretize.
     *                   Since the array will typically come from a program, attributes are
     *                   indexed from 0.
     * @throws IllegalArgumentException if an invalid set of ranges is supplied
     */
    public void setAttributeIndicesArray(int[] attributes) {

        setAttributeIndices(Range.indicesToRangeList(attributes));
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
     * Gets the bin ranges string for an attribute
     *
     * @param attributeIndex the index (from 0) of the attribute to get the bin
     *                       ranges string of
     * @return the bin ranges string (or null if the attribute requested has been
     * discretized into only one interval.)
     */
    public String getBinRangesString(int attributeIndex) {

        if (m_CutPoints == null) {
            return null;
        }

        double[] cutPoints = m_CutPoints[attributeIndex];

        if (cutPoints == null) {
            return "All";
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;

        for (int j = 0, n = cutPoints.length; j <= n; ++j) {
            if (first) {
                first = false;
            } else {
                sb.append(',');
            }

            sb.append(binRangeString(cutPoints, j, m_BinRangePrecision));
        }

        return sb.toString();
    }

    /**
     * Get a bin range string for a specified bin of some attribute's cut points.
     *
     * @param cutPoints The attribute's cut points; never null.
     * @param j         The bin number (zero based); never out of range.
     * @param precision the precision for the range values
     * @return The bin range string.
     */
    private static String binRangeString(double[] cutPoints, int j, int precision) {
        assert cutPoints != null;

        int n = cutPoints.length;
        assert 0 <= j && j <= n;

        return j == 0 ? "" + "(" + "-inf" + "-"
                + Utils.doubleToString(cutPoints[0], precision) + "]" : j == n ? "" + "("
                + Utils.doubleToString(cutPoints[n - 1], precision) + "-" + "inf" + ")"
                : "" + "(" + Utils.doubleToString(cutPoints[j - 1], precision) + "-"
                + Utils.doubleToString(cutPoints[j], precision) + "]";
    }


    /**
     * Set the output format. Takes the currently defined cutpoints and
     * m_InputFormat and calls setOutputFormat(Instances) appropriately.
     */
    protected weka.core.Instances changeOutputFormat(weka.core.Instances inputFormat) {

        if (m_CutPoints == null) {
            //setOutputFormat(null);
            return null;
        }
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(inputFormat.numAttributes());
        int classIndex = inputFormat.classIndex();
        for (int i = 0, m = inputFormat.numAttributes(); i < m; ++i) {
            if ((m_DiscretizeCols.isInRange(i))
                    && (inputFormat.attribute(i).isNumeric())) {

                Set<String> cutPointsCheck = new HashSet<String>();
                double[] cutPoints = m_CutPoints[i];

                ArrayList<String> attribValues;
                if (cutPoints == null) {
                    attribValues = new ArrayList<String>(1);
                    attribValues.add("'All'");
                } else {
                    attribValues = new ArrayList<String>(cutPoints.length + 1);
                    boolean predefinedLabels = false;
                    if (m_Labels != null && m_Labels[i] != null) {
                        if (m_Labels[i].length == m_CutPoints[i].length)
                            predefinedLabels = true;
                    }
                    if (predefinedLabels) {
                        for (int j = 0; j < m_Labels[i].length; j++) {
                            attribValues.add(m_Labels[i][j]);
                        }
                    } else {

                        for (int j = 0, n = cutPoints.length; j <= n; ++j) {
                            String newBinRangeString = binRangeString(cutPoints, j,
                                    m_BinRangePrecision);
                            if (cutPointsCheck.contains(newBinRangeString)) {
                                throw new IllegalArgumentException(
                                        "A duplicate bin range was detected. "
                                                + "Try increasing the bin range precision.");
                            }
                            attribValues.add("'" + newBinRangeString + "'");
                        }
                    }
                }

                //System.out.println("Att: " + i);
                //System.out.println("Att values: " + attribValues);
                Attribute newAtt = new Attribute(
                        inputFormat.attribute(i).name(), attribValues);
                newAtt.setWeight(inputFormat.attribute(i).weight());
                attributes.add(newAtt);

            } else {
                attributes.add((Attribute) inputFormat.attribute(i).copy());
            }
        }

        weka.core.Instances outputFormat = new weka.core.Instances(inputFormat.relationName(),
                attributes, 0);
        outputFormat.setClassIndex(classIndex);
        return outputFormat;
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
//                    if (instance.isMissing(i)) {
//                        vals[index] = Utils.missingValue();
//                    } else {
                        vals[index] = 0;
//                    }
                } else {
//                    if (instance.isMissing(i)) {
//                        vals[index] = Utils.missingValue();
//                    } else {
                        for (j = 0; j < m_CutPoints[i].length; j++) {
                            float cp = (float) (Math.round((double) m_CutPoints[i][j] * 1000000.0) / 1000000.0);
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
//                    }
                }
            } else {
                vals[index] = instance.vector().apply(i);
            }
            index++;
        }

        return new LabeledVector(instance.label(), new DenseVector(vals));
//        Instance outI = null;
//        if (instance instanceof SparseInstance) {
//            outI = new SparseInstance(instance.weight(), vals);
//        } else {
//            outI = new DenseInstance(instance.weight(), vals);
//        }
//
//        //copyValues(inst, false, instance.dataset(), outputFormatPeek());
//
//        WekaToSamoaInstanceConverter convWS = new WekaToSamoaInstanceConverter();
//        SamoaToWekaInstanceConverter convSW = new SamoaToWekaInstanceConverter();
//        LabeledVectorToSamoaInstanceConverter convLS = new LabeledVectorToSamoaInstanceConverter();
//
//        Instances outS = convWS.samoaInstances(changeOutputFormat(convSW.wekaInstances(instance.dataset())));
////        Instances outS = convLS.samoaInstances(changeOutputFormat())
//        outI.setDataset(outS);
//        return (outI);
    }

    /**
     * Test using Fayyad and Irani's MDL criterion.
     *
     * @param priorCounts
     * @param bestCounts
     * @param numInstances
     * @param numCutPoints
     * @return true if the splits is acceptable
     */
    protected boolean FayyadAndIranisMDL(double[] priorCounts,
                                         double[][] bestCounts, double numInstances, int numCutPoints) {

        double priorEntropy, entropy, gain;
        double entropyLeft, entropyRight, delta;
        int numClassesTotal, numClassesRight, numClassesLeft;

        // Compute entropy before split.
        priorEntropy = ContingencyTables.entropy(priorCounts);

        // Compute entropy after split.
        entropy = ContingencyTables.entropyConditionedOnRows(bestCounts);

        // Compute information gain.
        gain = priorEntropy - entropy;
        if (gain == priorEntropy)
            System.err.println("hOLA");

        // Number of classes occuring in the set
        numClassesTotal = 0;
        for (double priorCount : priorCounts) {
            if (priorCount > 0) {
                numClassesTotal++;
            }
        }

        // Number of classes occuring in the left subset
        numClassesLeft = 0;
        for (int i = 0; i < bestCounts[0].length; i++) {
            if (bestCounts[0][i] > 0) {
                numClassesLeft++;
            }
        }

        // Number of classes occuring in the right subset
        numClassesRight = 0;
        for (int i = 0; i < bestCounts[1].length; i++) {
            if (bestCounts[1][i] > 0) {
                numClassesRight++;
            }
        }

        // Entropy of the left and the right subsets
        entropyLeft = ContingencyTables.entropy(bestCounts[0]);
        entropyRight = ContingencyTables.entropy(bestCounts[1]);

        // Compute terms for MDL formula
        delta = Utils.log2(Math.pow(3, numClassesTotal) - 2)
                - ((numClassesTotal * priorEntropy) - (numClassesRight * entropyRight) - (numClassesLeft * entropyLeft));

        // Check if split is to be accepted
        return (gain > (Utils.log2(numCutPoints) + delta) / numInstances);
    }

    public int getNumberIntervals() {
        // TODO Auto-generated method stub
        if (m_CutPoints != null) {
            int ni = 0;
            for (double[] cp : m_CutPoints) {
                if (cp != null)
                    ni += (cp.length + 1);
            }
            return ni;
        }
        return 0;
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
        PrintWriter cpout1 = new PrintWriter(cpoints1);
        PrintWriter cpout2 = new PrintWriter(cpoints2);

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

    /**
     * Update the discretization model without updating
     *
     * @param inst
     */
    public abstract Float condProbGivenClass(int attI, double rVal, int dVal, int classVal, float classProb);

    public abstract Float condProbGivenClass(int attI, double rVal, int dVal, int classVal, int classCount);

    public abstract int getAttValGivenClass(int attI, double rVal, int dVal, int classVal);

    public abstract void updateEvaluator(LabeledVector inst);

    public abstract LabeledVector applyDiscretization(LabeledVector inst);

}
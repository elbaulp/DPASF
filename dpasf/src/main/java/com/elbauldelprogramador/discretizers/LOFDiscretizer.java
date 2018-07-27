package com.elbauldelprogramador.discretizers;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

import com.elbauldelprogramador.core.MOADiscretize;
import org.apache.flink.ml.common.LabeledVector;
import weka.core.Utils;

import com.yahoo.labs.samoa.instances.Instance;

/**
 * Local Online Fusion Discretizer<br/>
 * <br/>
 * <br/>
 * An online discretizer that fuse intervals locally using a measure based on quadratic entropy.<br/>
 * <p/>
 *
 * @author Sergio Ramírez (sramirez at decsai dot ugr dot es)
 */

public class LOFDiscretizer
        extends MOADiscretize
        implements Serializable {

    /**
     *
     */
    private int totalCount, numClasses, numAttributes;
    TreeMap<Float, Interval>[] allIntervals;
    private float lambda, alpha;
    private int initTh;
    private int maxHist;
    private int decimals;
    // Queue of labels for each attribute
    private Queue<Integer>[] labelsToUse;
    private LinkedList<Tuple<Float, Byte>>[] elemQ;
    private int maxLabels;
    private int[] contLabels;
    private int[][] classByAtt;


    /**
     * Default constructor. Parameters has been set to
     * the best configuration according to the authors.
     */
    public LOFDiscretizer() {
        // TODO Auto-generated constructor stub
        setAttributeIndices("first-last");
        this.alpha = 0.5f;
        this.lambda = 0.5f;
        this.initTh = 5000;
        this.maxHist = 10000;
        this.decimals = 3;
        this.maxLabels = 1000;
        this.provideProb = true;
    }

    /**
     * Second constructor. It allows to change the values of the user-defined parameters.
     * @param maxhist Maximum number of elements in each histogram inside intervals.
     * @param initTh Initial threshold which determines the number of instance before generating the first intervals.
     * @param decimals Number of decimals used to round points.
     * @param maxLabels Maximum number of labels to used in queues.
     */
    public LOFDiscretizer(int maxhist, int initTh, int decimals, int maxLabels, int numClasses) {
        this();
        this.initTh = initTh;
        this.decimals = decimals;
        this.maxHist = maxhist;
        this.maxLabels = maxLabels;
        this.numClasses = numClasses;
    }

    /**
     * Apply the discretization scheme to a new incoming instance.
     * The discretization scheme must be generated previously.
     * @param inst a new instance.
     * @return A new instance discretized.
     */
    public LabeledVector applyDiscretization(LabeledVector inst) {
        if(m_Init){
            for (int i = 0; i < inst.vector().size(); i++) {
                // if numeric and not missing, discretize
//                if(inst.attribute(i).isNumeric() && !inst.isMissing(i)) {
                    double[] boundaries = new double[allIntervals[i].size()];
                    String[] labels = new String[allIntervals[i].size()];
                    int j = 0;
                    for (Iterator<Interval> iterator = allIntervals[i].values().iterator(); iterator
                            .hasNext();) {
                        Interval interv = iterator.next();
                        labels[j] = Integer.toString(interv.label);
                        boundaries[j++] = interv.end;
                    }
                    m_Labels[i] = labels;
                    m_CutPoints[i] = boundaries;
//                }
            }
            return convertInstance(inst);
        }
        return inst;
    }

    /**
     *  Update the discretization scheme.
     *  @param instance an incoming instance.
     */
    public void updateEvaluator(LabeledVector instance) {
        int numAttributes = instance.vector().size();

        if(m_CutPoints == null) {
            initializeLayers(instance);
        }

        totalCount++;
        // Count number of elements per class
        for (int i = 0; i < numAttributes; i++) {
//            if(instance.attribute(i).isNumeric() && !instance.isMissing(i)) {
                classByAtt[i][(int) instance.label()]++;
//            }
        }

        // If there are enough instances to initialize cut points, do it!
        if(totalCount >= initTh){
            if(m_Init) {
                for (int i = 0; i < numAttributes; i++) {
//                    if(instance.attribute(i).isNumeric() && !instance.isMissing(i)) {
                        insertExample(i, instance);
//                    }
                }
                addExampleToQueue(instance);
            } else {
                addExampleToQueue(instance);
                batchFusinter(instance);

                m_Init = true;
            }
        } else {
            addExampleToQueue(instance);
        }
    }

    /**
     * Add values to the queues (by attribute). In this way, points are ordered by timestamp and
     * can be removed safely.
     * @param instance an incoming isntance.
     */
    private void addExampleToQueue(LabeledVector instance) {
        // Queue new values
        for (int i = 0; i < instance.vector().size(); i++) {
            // if numeric and not missing, discretize
//            if(instance.attribute(i).isNumeric() && !instance.isMissing(i)) {
                elemQ[i].add(new Tuple<Float, Byte>(
                        getInstanceValue(instance.vector().apply(i)), (byte)instance.label()));
//            }
        }
    }

    /**
     * Remove points from the queue till enough room is left for new points.
     * @param att Attribute index
     * @param interv Interval that needs exta space in its histogram.
     * @param size Number of spaces to free.
     */
    private void removeOldsUntilSize(int att, Interval interv, int size) {
        while(!elemQ[att].isEmpty() && interv.histogram.size() > size) {
            Tuple<Float, Byte> tuple = elemQ[att].poll();
            if(removePointFromInteravls(att, tuple))
                classByAtt[att][tuple.y]--;
        }
    }

    /**
     * Remove a point from the set of intervals in an attribute.
     * @param att Attribute index
     * @param elem Class and value of the point
     * @return if removal is performed
     */
    private boolean removePointFromInteravls(int att, Tuple<Float, Byte> elem) {
        Map.Entry<Float, Interval> ceilingE = allIntervals[att].ceilingEntry(elem.x);
        if(ceilingE != null){
            ceilingE.getValue().removePoint(att, elem.x, elem.y);
            // If interval is empty, remove it from the list
            if(ceilingE.getValue().histogram.isEmpty()) {
                allIntervals[att].remove(ceilingE.getKey());
                labelsToUse[att].add(ceilingE.getValue().label);
            }
            return true;
        }
        return false;
    }

    /**
     * Insert a new example in the discretization scheme. If it is a boundary point,
     * it is incorporated and a local fusion process is launched using this interval and
     * the surrounding ones. If not, the point just feed up the intervals.
     * @param att Attribute index
     * @param instance An incoming example
     */
    private void insertExample(int att, LabeledVector instance){

        int cls = (int) instance.label();
        float val = getInstanceValue(instance.vector().apply(att));
        // Get the ceiling interval for the given value
        Map.Entry<Float, Interval> centralE = allIntervals[att].ceilingEntry(val);
        // The point is within the range defined by centralE, if not a new maximum interval is created
        LinkedList<Interval> intervalList = new LinkedList<Interval>();
        if(centralE != null) {
            Interval central = centralE.getValue();
            // If it is a boundary point, evaluate six different cutting alternatives
            if(isBoundary(att, central, val, cls)){
                // Add splitting point before dividing the interval
                float oldKey = centralE.getKey();
                central.addPoint(att, val, cls); // do not remove any interval from allIntervals, all needed
                central.updateCriterion();
                // This instruction should be between the search of entries and after the addition of the point
                allIntervals[att].remove(oldKey);
                // Split the interval
                Interval splitI = central.splitInterval(att, val); // Criterion is updated for both intervals
                Map.Entry<Float, Interval> lowerE = allIntervals[att].lowerEntry(central.end);
                Map.Entry<Float, Interval> higherE = allIntervals[att].higherEntry(splitI.end); // if not use splitI, we will take again central
                // Insert in the specific order
                if(lowerE != null) {
                    intervalList.add(lowerE.getValue());
                    allIntervals[att].remove(lowerE.getKey());
                }
                intervalList.add(central);
                if(splitI != null)
                    intervalList.add(splitI);
                if(higherE != null) {
                    intervalList.add(higherE.getValue());
                    allIntervals[att].remove(higherE.getKey());
                }
                evaluateLocalMerges(att, intervalList);
                insertIntervals(att, intervalList);
            } else {
                // If not, just add the point to the interval
                central.addPoint(att, val, cls);
                central.updateCriterion();
                // Update the key with the bigger end
                if(centralE.getKey() != central.end) {
                    allIntervals[att].remove(centralE.getKey());
                    allIntervals[att].put(central.end, central);
                }
                intervalList.add(central);
            }
        } else {
            // New interval with a new maximum limit
            Map.Entry<Float, Interval> priorE = allIntervals[att].lowerEntry(val);
            // Insert in the specific order
            if(priorE != null) {
                intervalList.add(priorE.getValue());
                allIntervals[att].remove(priorE.getKey());
            }

            Interval nInt = new Interval(getLabel(att), val, cls);
            intervalList.add(nInt);
            evaluateLocalMerges(att, intervalList);
            insertIntervals(att, intervalList);
        }

        // Make an spot for new points, size limit's been achieved
        for (Iterator<Interval> iterator = intervalList.iterator(); iterator.hasNext();) {
            Interval interval = (Interval) iterator.next();
            if(interval.histogram.size() > maxHist) {
                removeOldsUntilSize(att, interval, maxHist);
            }
        }

    }

    /**
     * Write discretization points in this iteration (for two attributes) to file.
     * @param att1 Attribute index for first attribute.
     * @param att2 Attribute index for second attribute.
     * @param iteration Iteration ID
     */
    private void writeDataToFile(int att1, int att2, int iteration){
        FileWriter data = null;
        try {
            data = new FileWriter("Reb-data" + "-" + iteration + ".dat");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        PrintWriter dataout = new PrintWriter(data);

        for (int i = 0; i < elemQ.length; i++) {
            dataout.print(getInstanceValue(elemQ[att1].get(i).x) + "," +
                    getInstanceValue(elemQ[att2].get(i).x) + "," + getInstanceValue(elemQ[att1].get(i).y) + "\n");
        }

        //Flush the output to the file
        dataout.flush();

        //Close the Print Writer
        dataout.close();

        //Close the File Writer
        try {
            data.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Insert intervals in the global list of intervals.
     * @param att Attribute index
     * @param intervalList List of intervals to insert.
     */
    private void insertIntervals(int att, LinkedList<Interval> intervalList){
        for (Iterator<Interval> iterator = intervalList.iterator(); iterator
                .hasNext();) {
            Interval interval = iterator.next();
            if(interval != null)
                allIntervals[att].put(interval.end, interval);
        }

    }

    /**
     * Check if a point is boundary by firstly inspecting the closest interval to this point,
     * and then by checking the next entry in the histogram.
     * @param att Attribute index
     * @param ceiling The closest interval to the point.
     * @param value Point value
     * @param clas Class value
     * @return True if it is boundary, false otherwise.
     */
    private boolean isBoundary(int att, Interval ceiling, float value, int clas){

        boolean boundary = false;

        if(value != ceiling.end) {
            Entry<Float, int[]> following = ceiling.histogram.ceilingEntry(value);
            // The next point is in another interval (interval with a single point)
            if(following == null) {
                Entry<Float, Interval> higherE = allIntervals[att].higherEntry(ceiling.end);
                if(higherE == null) {
                    boundary = true; // no more points at the right side
                } else {
                    following = higherE.getValue().histogram.ceilingEntry(value);
                    int[] cd1 = following.getValue();
                    int[] cd2 = new int[cd1.length];
                    cd2[clas]++;
                    boundary = isBoundary(cd1, cd2);
                }
            } else {
                // If the point already exists before, evaluate if it is now a boundary
                if(following.getKey() == value) {

                    Entry<Float, int[]> nextnext = ceiling.histogram.higherEntry(value);
                    if(nextnext != null) {
                        int[] cd1 = following.getValue();
                        cd1[clas]++;
                        boundary = isBoundary(cd1, nextnext.getValue());
                        cd1[clas]--;
                    } else {
                        // Last point in the interval, it does not make sense to split
                        boundary = false;
                    }
                } else {
                    int[] cd1 = following.getValue();
                    int[] cd2 = new int[cd1.length];
                    cd2[clas]++;
                    boundary = isBoundary(cd1, cd2);
                }
            }
        }
        return boundary;

    }

    /**
     * Check histograms to assert the boundary condition.
     * @param cd1 Class histogram for point one.
     * @param cd2 Class histogram for point two.
     * @return @return True if it is boundary, false otherwise.
     */
    private boolean isBoundary(int[] cd1, int[] cd2){
        int count = 0;
        for (int i = 0; i < cd1.length; i++) {
            if(cd1[i] + cd2[i] > 0) {
                if(++count > 1) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Print basic information about a list of intervals
     * @param att Attribute index
     * @param intervals List of intervals
     */
    private void printIntervals(int att, Collection<Interval> intervals){
        System.out.println("Atributo: " + att);
        int sum = 0;
        for (Iterator<Interval> iterator = intervals.iterator(); iterator.hasNext();) {
            Interval interval = (Interval) iterator.next();
            for (int i = 0; i < interval.cd.length; i++) {
                sum += interval.cd[i];
            }
            System.out.println(interval.label + "-" + interval.end + "," + sum);
        }
    }

    /**
     * Apply standard FUSINTER to the first batch of examples. Then the discretization scheme
     * is initialized to apply further local changes.
     * @param model An example from this batch.
     */
    private void batchFusinter(LabeledVector model) {
        float[][] sorted = new float[numAttributes][];
        int nvalid = 0;
        LinkedList<Tuple<Float, Byte>> relem = null;
        for (int i = 0; i < elemQ.length; i++) {
            if(!elemQ[i].isEmpty()){
                relem = elemQ[i];
                break;
            }
        }

        if(relem == null)
            System.err.println("Error: No numerical attribute in the dataset.");

        final int[] classData = new int[relem.size()];
        for (int j = 0; j < relem.size(); j++) {
            classData[j] = relem.get(j).y;
            nvalid++;
        }

        for (int i = numAttributes - 1; i >= 0; i--) {
            if ((m_DiscretizeCols.isInRange(i))){
//                    && (model.attribute(i).isNumeric())
//                    && (model.classIndex() != i)) {
                Integer[] idx = new Integer[nvalid];
                sorted[i] = new float[elemQ[i].size()];
                for (int j = 0; j < idx.length; j++) {
                    idx[j] = j;
                    sorted [i][j] = getInstanceValue(elemQ[i].get(j).x);
                }
                final float[] data = sorted[i];

                // Order by feature value and class
                Arrays.sort(idx, new Comparator<Integer>() {
                    @Override
                    public int compare(final Integer o1, final Integer o2) {
                        int cmp_value = Float.compare(data[o1], data[o2]);
                        if(cmp_value == 0)
                            cmp_value = Integer.compare(classData[o1], classData[o2]);
                        return cmp_value;
                    }
                });

                allIntervals[i] = initIntervals(i, idx);
                printIntervals(i, allIntervals[i].values());
                ArrayList<Interval> intervalList = new ArrayList<Interval>(allIntervals[i].values());
                evaluateLocalMerges(i, intervalList);
                allIntervals[i] = new TreeMap<Float, Interval>();
                // Update keys in the tree map
                for (int j = 0; j < intervalList.size(); j++) {
                    allIntervals[i].put(intervalList.get(j).end, intervalList.get(j));
                }
                printIntervals(i, allIntervals[i].values());
            }
        }
    }

    /**
     * Apply local merges to a list of intervals until no more improvements can be obtained.
     * @param att Attribute index
     * @param intervalList List of intervals to merge
     */
    private void evaluateLocalMerges(int att, List<Interval> intervalList) {

        while(intervalList.size() > 1) {
            float globalDiff = 0;
            int posMin = 0;
            for(int i = 0; i < intervalList.size() - 1; i++) {
                float newLocalCrit = evaluteMerge(intervalList.get(i).cd, intervalList.get(i+1).cd);
                float difference = intervalList.get(i).crit + intervalList.get(i+1).crit - newLocalCrit;
                if(difference > globalDiff){
                    posMin = i;
                    globalDiff = difference;
                }
            }

            if(globalDiff > 0) {
                Interval int1 = intervalList.get(posMin);
                Interval int2 = intervalList.remove(posMin+1);
                int oldLabel = int1.mergeIntervals(int2);
                labelsToUse[att].add(oldLabel);
            } else {
                break;
            }
        }

    }

    /**
     * Evaluate a merge using both class histograms and the quadratic entropy measure.
     * @param cd1 Class histogram #1
     * @param cd2 Class histogram #2
     * @return Criterion value in case we merge both intervals.
     */
    private float evaluteMerge(int[] cd1, int[] cd2) {
        int[] cds = cd1.clone();
        for (int i = 0; i < cds.length; i++) {
            cds[i] += cd2[i];
        }
        return  evalInterval(cds);
    }

    /**
     * Quadratic entropy value for this interval.
     * @param cd Class histogram for this interval.
     * @return Criterion value.
     */
    private float evalInterval(int cd[]) {
        int Nj = 0;
        float suma, factor;
        for (int j = 0; j < numClasses; j++) {
            Nj += cd[j];
        }
        suma = 0;
        for (int j = 0; j < numClasses; j++) {
            factor = (cd[j] + lambda) / (Nj + numClasses * lambda);
            suma += factor * (1 - factor);
        }
        float crit = (alpha * ((float) Nj / totalCount) * suma) + ((1 - alpha) * (numClasses * lambda / Nj));
        return crit;
    }

    /**
     * Create the first intervals with the set of boundary points.
     * @param att Attribute index
     * @param idx Vector of indexes sorted by value.
     * @return A tree of tuples (boundary point, interval associated).
     */
    private TreeMap <Float, Interval> initIntervals(int att, Integer[] idx) {

        TreeMap <Float, Interval> intervals = new TreeMap<Float, Interval> ();
        LinkedList<Tuple<Float, int[]>> distinctPoints = new LinkedList<Tuple<Float, int[]>>();
        float valueAnt = getInstanceValue(elemQ[att].get(idx[0]).x);
        int classAnt = elemQ[att].get(idx[0]).y;
        int[] cd = new int[numClasses];
        cd[classAnt]++;
        // Compute statically the set of distinct points (boundary)
        for(int i = 1; i < idx.length;i++) {
            float val = getInstanceValue(elemQ[att].get(idx[i]).x);
            int clas = elemQ[att].get(idx[i]).y;
            if(val == valueAnt) {
                cd[clas]++;
            } else {
                distinctPoints.add(new Tuple<Float, int[]>(valueAnt, cd));
                cd = new int[numClasses];
                cd[clas]++;
                valueAnt = val;
            }

        }
        distinctPoints.add(new Tuple<Float, int[]>(valueAnt, cd));

        // Filter only those that are on the class boundaries
        Interval interval = new Interval(getLabel(att));
        Tuple<Float, int[]> t1 = distinctPoints.get(0);
        interval.addPoint(t1);

        for (int i = 1; i < distinctPoints.size(); i++) {
            Tuple<Float, int[]> t2 = distinctPoints.get(i);
            if(isBoundary(t1.y, t2.y)){
                // Compute the criterion value and add them to the pool
                interval.updateCriterion(); // Important!
                intervals.put(t1.x, interval);
                interval = new Interval(getLabel(att));
            }
            interval.addPoint(t2);
            t1 = t2;
        }
        // Add the last criterion to the pool
        interval.updateCriterion(); // Important!
        intervals.put(t1.x, interval);
        return intervals;
    }

    /**
     * Initialize all the variables during the first stage.
     * @param inst The first example
     */
    private void initializeLayers(LabeledVector inst) {
        m_DiscretizeCols.setUpper(inst.vector().size());
//        numClasses = inst.numClasses();
        numAttributes = inst.vector().size();
        allIntervals = new TreeMap[numAttributes];
        m_CutPoints = new double[numAttributes][];
        m_Labels = new String[numAttributes][];
        elemQ = new LinkedList[numAttributes];
        labelsToUse = new Queue[numAttributes];
        contLabels = new int[numAttributes];
        classByAtt = new int[numAttributes][];

        for (int i = 0; i < inst.vector().size(); i++) {
            allIntervals[i] = new TreeMap<Float, Interval>();
            elemQ[i] = new LinkedList<Tuple<Float, Byte>>();
            labelsToUse[i] = new LinkedList<Integer>();
            contLabels[i] = maxLabels;
            classByAtt[i] = new int[numClasses];
            for (int j = 1; j < maxLabels + 1; j++) {
                labelsToUse[i].add(j);
            }
        }
    }

    /**
     * Transform a point by rounding it according to the number of decimals
     * defined as input parameter.
     * @param value Value to transform
     * @return Rounded value.
     */
    private float getInstanceValue(double value) {
        if(decimals > 0) {
            double mult = Math.pow(10, decimals);
            return (float) (Math.round(value * mult) / mult);
        }
        return (float) value;
    }

    @Override
    public Float condProbGivenClass(int attI, double rVal, int dVal, int classVal, float classProb) {
        // TODO Auto-generated method stub
        float joint = getAttValGivenClass(attI, rVal, dVal, classVal);
        if(joint > 0) {
            joint /= Utils.sum(classByAtt[attI]);
            return joint / classByAtt[attI][classVal];
        }
        return null;
    }

    public Float condProbGivenClass(int attI, double rVal, int dVal, int classVal, int ClassCount) {
        // TODO Auto-generated method stub
        float joint = getAttValGivenClass(attI, rVal, dVal, classVal);
        return (joint + 1.0f) / (classByAtt[attI][classVal] + allIntervals[attI].size());
    }

    @Override
    public int getAttValGivenClass(int attI, double rVal, int dVal, int classVal) {
        Map.Entry<Float, Interval> centralE = allIntervals[attI]
                .ceilingEntry(getInstanceValue(rVal));
        if(centralE != null) {
            return centralE.getValue().cd[classVal];
        }
        return 0;
    }

    /**
     * Get a new label for an interval. It is taken from
     * the queue of labels if this is not empty. If not,
     * a new label equal to the current limit + 1 is returned.
     * @param att Attribute index
     * @return The label value to use.
     */
    private int getLabel(int att){
        if(labelsToUse[att].isEmpty())
            return ++contLabels[att];
        return labelsToUse[att].poll();
    }

    class Interval {
        /**
         * <p>
         * Class that represent an interval determined by a rightmost boundary point and
         * an histogram of compounded points.
         * </p>
         */
        int label;
        float end;
        int [] cd;
        TreeMap<Float, int[]> histogram;
        float crit;

        public Interval() {
            // TODO Auto-generated constructor stub
        }

        public Interval(int _label) {
            label = _label;
            end = -1;
            histogram = new TreeMap<Float, int[]>();
            cd = new int[numClasses];
            crit = Float.MIN_VALUE;
        }

        /**
         * <p>
         * Compute the interval ratios.
         * </p>
         * @param _attribute
         * @param []_values
         * @param _begin
         * @param _end
         */
        public Interval(int _label, float _end, int _class) {
            label = _label;
            end = _end;
            histogram = new TreeMap<Float, int[]>();
            cd = new int[numClasses];
            cd[_class] = 1;
            histogram.put(_end, cd.clone());
            crit = Float.MIN_VALUE;
        }

        public Interval(Interval other) {
            // TODO Auto-generated constructor stub
            label = other.label;
            end = other.end;
            cd = other.cd.clone();
            crit = other.crit;
            histogram = new TreeMap<Float, int[]>();
            histogram.putAll(other.histogram);
        }

        public void addPoint(int att, float value, int cls){
            int[] pd = histogram.get(value);
            if(pd != null) {
                pd[cls]++;
            } else {
                pd = new int[cd.length];
                pd[cls]++;
                histogram.put(value, pd);
            }
            // Update values
            cd[cls]++;
            if(value > end)
                end = value;
        }

        public void addPoint(float value, int cd[]){
            int[] prevd = histogram.get(value);
            if(prevd == null) {
                prevd = new int[numClasses];
            }

            for (int i = 0; i < cd.length; i++)
                prevd[i] += cd[i];
            histogram.put(value, prevd);

            for (int i = 0; i < cd.length; i++) {
                this.cd[i] += cd[i];
            }

            if(value > end)
                end = value;
        }

        public void addPoint(Tuple<Float, int[]> point){
            addPoint(point.x, point.y);
        }

        public void removePoint(int att, float value, int cls) {
            int[] pd = histogram.get(value);
            if(pd != null) {
                if(pd[cls] > 0) {
                    pd[cls]--;
                    cd[cls]--;
                } else {
                    System.err.println("Bad histogram.");
                }
                boolean delete = true;
                // If all values are equal to zero, remove the point from the map
                for (int i = 0; i < pd.length; i++) {
                    if(pd[i] > 0){
                        delete = false;
                        break;
                    }
                }
                if(delete){ // We keep intervals with at least one point
                    histogram.remove(value);
                }
            } else {
                // Error, no point in this range.
                System.err.println("Point not found in the given range.");
            }
            // Find a new maximum if the point removed is the maximum
            if(value == end) {
                Float newend = histogram.floorKey(value); // get the new maximum
                if(newend != null)
                    end = newend;
            }
        }

        public Interval splitInterval(int att, float value) {

            TreeMap<Float, int[]> nHist = new TreeMap<Float, int[]>();
            int[] nCd = new int[cd.length];

            for (Iterator<Entry<Float, int[]>> iterator =
                 histogram.tailMap(value, false).entrySet().iterator(); iterator.hasNext();) {
                Entry<Float, int[]> entry = (Entry) iterator.next();
                nHist.put(entry.getKey(), entry.getValue());
                for(int i = 0; i < entry.getValue().length; i++){
                    nCd[i] += entry.getValue()[i];
                    cd[i] -= entry.getValue()[i];
                }
                iterator.remove();
            }

            if(nHist.isEmpty())
                return null;

            /** New interval (which lays at the right of the cut point) **/
            int s1 = 0, s2 = 0;
            for (int i = 0; i < nCd.length; i++) {
                s1 += cd[i];
                s2 += nCd[i];
            }
            // Label management
            Interval nInterval = new Interval();
            if(s1 > s2){
                nInterval.label = getLabel(att);
            } else {
                nInterval.label = this.label;
                this.label = getLabel(att);
            }
            nInterval.cd = nCd;
            nInterval.histogram = nHist;
            nInterval.end = this.end;
            nInterval.updateCriterion();
            /** Old interval (which lays at the left of the cut point) **/
            this.end = value;
            updateCriterion();

            return nInterval;
        }

        public int mergeIntervals(Interval interv2){
            // The interval with less elements lose its label
            int s1 = 0, s2 = 0;
            for (int i = 0; i < cd.length; i++) {
                s1 += cd[i];
                s2 += interv2.cd[i];
            }

            int oldlab = interv2.label;
            if(s1 < s2) {
                oldlab = this.label;
                this.label = interv2.label;
            }

            // Set the new end
            end = (this.end > interv2.end) ? this.end : interv2.end;

            // Merge histograms and class distributions
            for (int i = 0; i < cd.length; i++) {
                cd[i] += interv2.cd[i];
            }
            for (Iterator iterator = interv2.histogram.entrySet().iterator(); iterator.hasNext();) {
                Entry<Float, int[]> entry = (Entry) iterator.next();
                int[] v = histogram.get(entry.getKey());
                if(v != null) {
                    for (int i = 0; i < v.length; i++){
                        v[i] += entry.getValue()[i];
                    }
                } else {
                    histogram.put(entry.getKey(), entry.getValue());
                }
            }

            updateCriterion();
            return oldlab;
        }

        public void setEnd(float end) {
            this.end = end;
        }

        public void setCrit(float crit) {
            this.crit = crit;
        }

        public void updateCriterion(){
            this.crit = evalInterval(cd);
        }

    }


    public class Tuple<X, Y> {
        public final X x;
        public final Y y;
        public Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }


}
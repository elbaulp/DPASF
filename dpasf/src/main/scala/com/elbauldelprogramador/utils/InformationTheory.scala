package com.elbauldelprogramador.utils

import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import breeze.linalg.DenseVector
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{ Vector ⇒ FlinkVec }

/**
 * Object containing utility functions for Information Theory
 */
case object InformationTheory {
  import IFImplicits._

  private[this] val log = LoggerFactory.getLogger(getClass)

  private[this] val log2 = math.log(2)

  private[this] def log2(a: Double): Double = math.log(a) / log2
  private[this] def log2(a: DenseVector[Double]): DenseVector[Double] = a.map(log2)

  private[this] def log(x: Double): Double =
    if (x > 0) math.log(x)
    else 0

  private[this] val nlogn = (x: Double) ⇒ x * log(x)
  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   * @param n     Number of elements
   *
   */
  private[this] def entropy(freqs: Seq[Double], n: Double) = {
    freqs.aggregate(0.0)({
      case (h, q) ⇒
        h + (if (q == 0) 0 else (q.toDouble / n) * (math.log(q.toDouble / n) / math.log(2)))
    }, { case (h1, h2) ⇒ h1 + h2 }) * -1
  }

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   */
  def entropy(freqs: Seq[Double]): Double =
    entropy(freqs, freqs.sum)

  /**
   * Computes entropy of the a single-column [[DataSet]]
   *
   * @param x [[DataSet]] with one column
   * @return its entropy
   */
  def entropy(x: DataSet[Double]): Double = {
    val p: DenseVector[Double] = probs(x)
    p dot (-log2(p))
  }

  /**
   * Compute the probabilities of each value on the given [[DataSet]]
   *
   * @param x single colum [[DataSet]]
   * @return Sequence of probabilites for each value
   */
  private[this] def probs(x: DataSet[Double]): Seq[Double] = {
    val counts = x.groupBy(x ⇒ x)
      .reduceGroup(_.size.toDouble).collect
    val total = counts.sum

    counts.map(_ / total)
  }

  /**
   * Computes conditional entropy of the columns given
   * the rows.
   *
   * @param freqs the contingency table
   * @return the conditional entropy of the columns given the rows
   */
  def entropyConditionedOnRows(freqs: Seq[Seq[Double]]): Double = {
    val total = freqs.map(_.sum).sum
    -freqs.aggregate(.0)({
      case (h, q) ⇒
        (h + q.map(nlogn).sum) - nlogn(q.sum)
    }, {
      case (h1, h2) ⇒
        h1 + h2
    }) / (total * log2)
  }

  /**
   * Computes conditional entropy H(X|Y) for the given [[DataSet]]
   *
   * @param xy [[DataSet]] with two columns, its a (Double, Double), where
   * Y should be on the left, X on the right in order to compute H(X|Y)
   * @return Conditional Entropy H(X|Y)
   */
  def conditionalEntropy(xy: DataSet[(Double, Double)]): Double = {
    val y = xy map (_._1)
    val p = probs(y).toArray.asBreeze
    val values = y.distinct.collect
    val condH = for (i ← values)
      yield entropy(xy.filter(_._1 == i).map(_._2))

    p.dot(seq2Breeze(condH))
  }

  /**
   * Computes conditional entropy H(X|Y) for the given [[DataSet]]
   *
   * @param y  [[DataSet]] with the values for y, single column
   * @param xy [[DataSet]] with two columns, its a (Double, Double), where
   * Y should be on the left, X on the right in order to compute H(X|Y)
   * @return Conditional Entropy H(X|Y)
   */
  def conditionalEntropy(y: DataSet[Double], xy: DataSet[(Double, Double)]): Double = {
    val p = probs(y).toArray.asBreeze
    val values = y.distinct.collect
    val condH = for (i ← values)
      yield entropy(xy.filter(_._1 == i).map(_._2))

    p.dot(seq2Breeze(condH))
  }

  /**
   * Returns the Mutual Information for the given two columns [[DataSet]]
   *
   * Mutual Information is defined as H(X) - H(X|Y)
   *
   * @param xy [[DataSet]] with two colums, its a (Double, Double), where
   * Y should be on the left and X on the right.
   * @return Mutual Information
   */
  def mutualInformation(xy: DataSet[(Double, Double)]): Double =
    entropy(xy map (_._2)) - conditionalEntropy(xy)

  /**
   * Returns the Mutual Information for the given two columns [[DataSet]]
   *
   * Mutual Information is defined as H(X) - H(X|Y)
   *
   * @param x [[DataSet]] with one column
   * @param xy [[DataSet]] with two colums, its a (Double, Double), where
   * Y should be on the left and X on the right.
   * @return Mutual Information
   */
  def mutualInformation(x: DataSet[Double], xy: DataSet[(Double, Double)]): Double =
    entropy(x) - conditionalEntropy(xy)

  /**
   * Returns the Mutual Information for the given two columns [[DataSet]]
   *
   * Mutual Information is defined as H(X) - H(X|Y)
   *
   * @param x [[DataSet]] with values for x, single column
   * @param y [[DataSet]] with values for y, single column
   * @param xy [[DataSet]] with two colums, its a (Double, Double), where
   * Y should be on the left and X on the right.
   * @return Mutual Information
   */
  def mutualInformation(x: DataSet[Double], y: DataSet[Double], xy: DataSet[(Double, Double)]): Double =
    entropy(x) - conditionalEntropy(y, xy)

  /**
   * Computes 'symmetrical uncertainty' (SU) - a symmetric mutual information measure.
   *
   * It is defined as SU(X, y) = 2 * (IG(X|Y) / (H(X) + H(Y)))
   *
   * @param xy [[DataSet]] with two features
   * @return SU value
   */
  def symmetricalUncertainty(xy: DataSet[(Double, Double)]): Double = {
    val y = xy map (_._1)
    val x = xy map (_._2)
    2 * mutualInformation(x, y, xy) / (entropy(x) + entropy(y))
  }

  /**
   * Test using Fayyad and Irani's MDL criterion.
   *
   * @param priorCounts
   * @param bestCounts
   * @param numInstances
   * @param numCutPoints
   *
   * @return true if the splits is acceptable
   */
  def FayyadAndIranisMDL(
    priorCounts: Seq[Double],
    bestCounts: Seq[Seq[Double]],
    numInstances: Double,
    numCutPoints: Int): Boolean = {
    // Entropy before split
    val priorH = entropy(priorCounts)

    // Entropy after split
    val h = entropyConditionedOnRows(bestCounts)

    // Compute InfoGain
    val gain = priorH - h

    // Number of classes occuring in the set
    val nClasses = priorCounts.count(_ != 0)

    // Number of classes in the left subset
    val nClassesLeft = bestCounts.head.count(_ != 0)
    // Number of classes in the right subset
    val nClassesRight = bestCounts(1).count(_ != 0)

    // Entropies for left and right
    val hLeft = entropy(bestCounts.head)
    val hRight = entropy(bestCounts(1))

    // MDL formula
    val delta = log2(math.pow(3, nClasses) - 2) -
      ((nClasses * priorH) - (nClassesRight * hRight) - (nClassesLeft * hLeft))

    // Check if split is accepted or not
    //    gain > ((log2(numInstances - 1) + delta) / numInstances)
    gain > ((log2(numCutPoints - 1) + delta) / numInstances)
  }

}

object IFImplicits {
  // Implicits
  //implicit def flinkVec2Vec(x: FlinkVec): Vector[Double] = x.toVector
  implicit def linalgVec2Vec(x: DenseVector[Double]): Seq[Double] = x.toSeq
  implicit def seq2Breeze(x: Seq[Double]): DenseVector[Double] = x.toArray.asBreeze
  //implicit def vec2LinalgVec(x: Seq[Double]): DenseVector[Double] = DenseVector(x.toArray)
}

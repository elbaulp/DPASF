package com.elbauldelprogramador.utils

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{ DenseVector, Vector }
import org.slf4j.LoggerFactory

import scala.collection.GenTraversable

/**
 * Object containing utility functions for Information Theory
 */
case object InformationTheory {

  private[this] val log = LoggerFactory.getLogger(getClass)

  private[this] val log2 = math.log(2)

  private[this] def log2(a: Double): Double = math.log(a) / log2

  private[this] def log2(a: Vector): Vector = DenseVector(a.map(v ⇒ log2(v._2)).toArray)

  private[this] def minuslog2(a: Vector): Vector = DenseVector(a.map(v ⇒ -log2(v._2)).toArray)

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
  private[this] def entropy(freqs: GenTraversable[Double], n: Double) = {
    freqs.aggregate(0.0)({
      case (h, q) ⇒
        h + (if (q == 0) 0 else (q.toDouble / n) * (math.log(q.toDouble / n) / math.log(2)))
    }, { case (h1, h2) ⇒ h1 + h2 }) * -1
  }

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param x Frequencies of each different class
   */
  def entropy(x: GenTraversable[Double]): Double = {
    val p = probs(x)
    p.dot(minuslog2(p))
  }

  /**
   * Compute the probabilities of each value on the given [[collection]]
   *
   * @param x single column [[collection]]
   * @return [[Vector]] of probabilities for each value
   */
  private[this] def probs(x: GenTraversable[Double]): Vector = {
    val counts = x.groupBy(identity)
      .map(_._2.size)

    val ps = counts map (_ / counts.sum.toDouble)

    DenseVector(ps.toArray)
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
   * Computes conditional entropy H(X|Y) for the given two [[collection]]
   *
   * @param x [[Seq]] with the values for x, single column
   * @param y [[Seq]] with the values for y
   * @return Conditional Entropy H(X|Y)
   */
  def conditionalEntropy(x: Seq[Double], y: Seq[Double]): Double = {
    val p = probs(y)
    val values = y.distinct
    val xy = x zip y
    val condH = for (i ← values)
      yield entropy(xy filter (_._2 == i) map (_._1))

    val condHVec = DenseVector(condH.toArray)

    p dot condHVec
  }

  /**
   * Returns the Mutual Information for the given two [[collection]]
   *
   * Mutual Information is defined as H(X) - H(X|Y)
   *
   * @param x  [[Seq]] with one column, representing X
   * @param y  [[Seq]] with one colums, representing y
   * @return Mutual Information
   */
  def mutualInformation(x: Seq[Double], y: Seq[Double]): Double =
    entropy(x) - conditionalEntropy(x, y)

  /**
   * Computes 'symmetrical uncertainty' (SU) - a symmetric mutual information measure.
   *
   * It is defined as SU(X, y) = 2 * (IG(X|Y) / (H(X) + H(Y)))
   *
   * @param xy [[DataSet]] with two features
   * @return SU value
   */
  def symmetricalUncertainty(xy: DataSet[(Double, Double)]): Double = {
    val su = xy.reduceGroup { in ⇒
      val invec = in.toVector
      val x = invec map (_._2)
      val y = invec map (_._1)

      val mu = mutualInformation(x, y)
      val Hx = entropy(x)
      val Hy = entropy(y)

      2 * mu / (Hx + Hy)
    }

    su.collect.head
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

package com.elbauldelprogramador.utils

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.ml.math.{ DenseVector, Vector }
import org.slf4j.LoggerFactory

import com.elbauldelprogramador.utils.Utils._

import scala.collection.{ GenTraversable, mutable }

/**
 * Object containing utility functions for Information Theory
 */
case object InformationTheory {

  private[this] val log = LoggerFactory.getLogger(getClass)

  private[this] val log2 = math.log(2)

  private[this] def log2(a: Double): Double = if (a > 0) math.log(a) / log2 else 0

  private[this] def log2(a: Vector): Vector = DenseVector(a.map(v ⇒ log2(v._2)).toArray)

  private[this] def minuslog2(a: Vector): Vector = DenseVector(a.map(v ⇒ -log2(v._2)).toArray)

  private[this] def log(x: Double): Double =
    if (x > 0) math.log(x)
    else 0

  private[this] val nlogn = (x: Double) ⇒ x * log(x)

  private[this] val entropyCache = mutable.Map.empty[Int, Double]

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   * @param n     Number of elements
   *
   */
  def entropy(freqs: GenTraversable[Double], n: Double) = {
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
  def entropy(freqs: GenTraversable[Double]): Double = {
    val k = freqs.##
    if (entropyCache.isDefinedAt(k)) {
      log.debug(" Cache Hit for entropy!")
      entropyCache(k)
    } else {
      val p = DenseVector(freqs.toArray)
      val h = p.dot(minuslog2(p))
      entropyCache(k) = h
      h
    }
  }

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param x Frequencies of each different class
   */
  //ef entropy(x: GenTraversable[Double])(a: DummyImplicit): Double = {
  // val k = x.##
  // if (entropyCache.isDefinedAt(k)) {
  //   log.debug(" Cache Hit for entropy!")
  //   entropyCache(k)
  // } else {
  //   val p = probs(x)
  //   val h = p.dot(minuslog2(p))
  //   entropyCache(k) = h
  //   h
  // }
  //

  /** hot fix */

  def entropy(x: DataSet[Double]): DataSet[Double] = {
    val p = probs2(x)
    p.map(x ⇒ x.dot(minuslog2(x)))
  }

  def probs2(x: DataSet[Double]): DataSet[DenseVector] = {
    val r = x.reduceGroup { in ⇒
      val v = in.toVector
      val r = v map (_ / v.sum)
      DenseVector(r.toArray)
    }
    r
  }

  def probs2(x: GenTraversable[Double]): Vector = {
    val counts = x.groupBy(identity)
      .map(_._2.size)

    val ps = counts map (_ / counts.sum.toDouble)

    DenseVector(ps.toArray)
  }

  /** / hot fix */

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
   * @param x [[Seq]] with one column, representing X
   * @param y [[Seq]] with one colums, representing y
   * @return Mutual Information
   */
  def mutualInformation(x: Seq[Double], y: Seq[Double]): Double =
    entropy(x) - conditionalEntropy(x, y)

  /**
   * Computes 'symmetrical uncertainty' (SU) - a symmetric mutual information measure.
   *
   * It is defined as SU(X, y) = 2 * (IG(X|Y) / (H(X) + H(Y)))
   *
   * @param yx [[DataSet]] with two features
   * @return SU value
   */
  def symmetricalUncertainty(yx: DataSet[(Double, Double)]): Double = {
    val su =
      // First Compute px and py
      yx.mapPartitionWith {
        case yx ⇒
          val x = yx map (_._2)
          val y = yx map (_._1)

          val xPartialCounts = x.groupBy(identity)
            .map { case (id, v) ⇒ (id, v.size) }
          val yPartialCounts = y.groupBy(identity)
            .map { case (id, v) ⇒ id -> v.size }

          (xPartialCounts, yPartialCounts, x, y)

      }.name("PartialCounts")
        // Merge all probailities
        .reduceWith {
          case (a, b) ⇒

            val x = for ((k, v) ← a._1) yield k -> (b._1.getOrElse(k, 0) + v)
            val y = for ((k, v) ← a._2) yield k -> (b._2.getOrElse(k, 0) + v)

            (x, y, a._3, a._4)

        }.name("Total Counts")
        // Compute H(X), H(Y), MU and finally SU
        .mapWith {
          case (xCounts, yCounts, x, y) ⇒

            val total = yCounts.values.sum.toDouble

            val px = xCounts.map(x ⇒ x._1 -> x._2 / total).values
            val hx = entropy(px)

            val py = yCounts.map(x ⇒ x._1 -> x._2 / total).values
            val hy = entropy(py)

            val mu = mutualInformation(x, y)

            2 * mu / (hx + hy)
        }.name("MU & SU")
    su.collect.head
  }

  // def time[R](desc: String)(block: ⇒ R): R = {
  //   val t0 = System.nanoTime()
  //   val result = block // call-by-name
  //   val t1 = System.nanoTime()
  //   val execTime = (t1 - t0) / 1e9
  //   log.info(s"$execTime for $desc")
  //   writeToFile(s"/home/aalcalde/times2/$desc", execTime.toString)
  //   //    println(s"Elapsed time for $desc: " + (t1 - t0) + " ns")
  //   result
  // }

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
    val nClassesLeft = bestCounts.headOption.map(_.count(_ != 0)).getOrElse(0)
    // Number of classes in the right subset
    val nClassesRight = bestCounts.lastOption.map(_.count(_ != 0)).getOrElse(0)

    // Entropies for left and right
    val hLeft = entropy(bestCounts.headOption.getOrElse(Seq.empty))
    val hRight = entropy(bestCounts.lastOption.getOrElse(Seq.empty))

    // MDL formula
    val delta = log2(math.pow(3, nClasses) - 2) -
      ((nClasses * priorH) - (nClassesRight * hRight) - (nClassesLeft * hLeft))

    // Check if split is accepted or not
    //    gain > ((log2(numInstances - 1) + delta) / numInstances)
    gain > ((log2(numCutPoints - 1) + delta) / numInstances)
  }

}

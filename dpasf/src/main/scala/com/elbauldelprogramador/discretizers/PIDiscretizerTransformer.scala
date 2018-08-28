/*
 * Copyright (C) 2018  Alejandro Alcalde
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.elbauldelprogramador.discretizers

import com.elbauldelprogramador.datastructures.Histogram
import com.elbauldelprogramador.utils.{ FlinkUtils, InformationTheory }
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{ LabeledVector, Parameter, ParameterMap }
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }

import scala.collection.mutable.ArrayBuffer

/**
 * Partition Incremental Discretization (PiD)
 *
 * For more information, see:<br/>
 *
 * João Gama and Carlos Pinto. 2006. Discretization from data streams: applications to histograms and data mining.
 * In Proceedings of the 2006 ACM symposium on Applied computing (SAC '06). ACM, New York, NY, USA, 662-667.
 *
 * DOI : http://dx.doi.org/10.1145/1141277.1141429
 *
 */
class PIDiscretizerTransformer extends Transformer[PIDiscretizerTransformer] {

  import PIDiscretizerTransformer._

  //  private[PIDiscretizerTransformer] var metricsOption: Option[DataSet[Vector[Vector[Double]]]] = None
  private[PIDiscretizerTransformer] lazy val step = (parameters(Max) - parameters(Min)) / parameters(L1InitialBins).toDouble

  /**
   * Sets the number of Samples needed to be observer to trigger
   * a layer 2 update
   *
   * @param l2Updates Number of instances needed to trigger a Layer 2 update
   * @return [[PIDiscretizerTransformer]]
   */
  def setUpdateExamples(l2Updates: Int): PIDiscretizerTransformer = {
    parameters add (L2UpdateExamples, l2Updates)
    this
  }

  /**
   * Sets the number of initial bins for layer 1
   *
   * @param bins Number of bins
   * @return [[PIDiscretizerTransformer]]
   */
  def setL1Bins(bins: Int): PIDiscretizerTransformer = {
    parameters add (L1InitialBins, bins)
    this
  }

  /**
   * Sets the minimun number of elements to start the update for layer 1
   *
   * @param n Number of elements
   * @return [[PIDiscretizerTransformer]]
   */
  def setInitElements(n: Int): PIDiscretizerTransformer = {
    parameters add (InitialElements, n)
    this
  }

  /**
   * Set the threshold for splitting a interval
   *
   * @param alpha Desired alpha threshold
   * @return [[PIDiscretizerTransformer]]
   */
  def setAlpha(alpha: Double): PIDiscretizerTransformer = {
    parameters add (Alpha, alpha)
    this
  }

  /**
   * If data not scaled, use this as the minimun variable range
   *
   * currently not used, its assumed the data is scaled
   *
   * @param min min value of the variable
   * @return [[PIDiscretizerTransformer]]
   */
  def setMin(min: Int): PIDiscretizerTransformer = {
    parameters add (Min, min)
    this
  }

  /**
   * If data not scaled, use this as the maximun variable range
   *
   * currently not used, its assumed the data is scaled
   *
   * @param max max value of the variable
   * @return [[PIDiscretizerTransformer]]
   */
  def setMax(max: Int): PIDiscretizerTransformer = {
    parameters add (Max, max)
    this
  }
}

object PIDiscretizerTransformer {

  //  private[this] val log = LoggerFactory.getLogger(this.getClass)

  // ========================================== Parameters =========================================
  private[PIDiscretizerTransformer] case object L2UpdateExamples extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(1000)
  }

  private[PIDiscretizerTransformer] case object Alpha extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(.75)
  }

  private[PIDiscretizerTransformer] case object Min extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(0)
  }

  private[PIDiscretizerTransformer] case object Max extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(1)
  }

  private[PIDiscretizerTransformer] case object L1InitialBins extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(200)
  }

  private[PIDiscretizerTransformer] case object InitialElements extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(100)
  }

  // ========================================== Factory methods ====================================
  def apply(): PIDiscretizerTransformer = new PIDiscretizerTransformer

  // ========================================== Operations =========================================

  /**
   * [[PIDiscretizerTransformer]] does not need a fitting phase
   */
  implicit val fitNoOp = new FitOperation[PIDiscretizerTransformer, LabeledVector] {
    override def fit(
      instance: PIDiscretizerTransformer,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector]): Unit = {}
  }

  /**
   * [[TransformDataSetOperation]] which performs a Discretization using PiD (Partition Incremental Discretization) on
   * a [[DataSet]] which type is [[LabeledVector]]
   *
   * @return A [[DataSet]] discretized according to PiD
   */
  implicit val transformLabeledIDADiscretizer = new TransformDataSetOperation[PIDiscretizerTransformer, LabeledVector, LabeledVector] {
    override def transformDataSet(
      instance: PIDiscretizerTransformer,
      transformParameters: ParameterMap,
      input: DataSet[LabeledVector]): DataSet[LabeledVector] = {
      val resultingParameters = instance.parameters ++ transformParameters

      val l2updateExamples = resultingParameters(L2UpdateExamples)
      val alpha = resultingParameters(Alpha)
      val min = resultingParameters(Min)
      val max = resultingParameters(Max)
      val l1InitialBins = resultingParameters(L1InitialBins)
      val initialElems = resultingParameters(InitialElements)
      val nAttrs = FlinkUtils.numAttrs(input)

      val metric = input.map { x ⇒
        // (instance, histrogram totalCount)
        (x, Histogram(nAttrs, l1InitialBins, min, instance.step), 1)
      }.reduce { (m1, m2) ⇒

        // Update Layer 1
        val updatedL1 = updateL1(m1._1, m1._2, instance.step, initialElems, alpha, m1._3)

        // Update Layer 2 if necesary
        val updatedL2 = if (m1._3 % l2updateExamples == 0) {
          updateL2(m1._1, updatedL1)
        } else updatedL1

        (m2._1, updatedL2, m1._3 + 1)
      }.map(_._2.cutMatrixL2.map(_.toVector).toVector)

      input.mapWithBcVariable(metric) { (lv, cuts) ⇒

        val LabeledVector(label, vector) = lv
        val discretized = vector.map {
          case (i, v) ⇒
            // Get the bin
            cuts(i).indexWhere(v <= _).toDouble
        }
        LabeledVector(label, DenseVector(discretized.toArray))
      }
    }
  }

  /**
   * Updates Layer 1 of the algorithm.
   *
   * This layer simplifies and summarizes the data. In this phase there are usualy
   * much more bins that desired, specified via [[L1InitialBins]].
   *
   * This method:
   *
   * - For each value on the [[DataSet]], this method determines its corresponding interval.
   * - When the numbers of counts is above [[Alpha]] (A percentage of the total number of points seen so far)
   * a split process is triggered.  If the interval triggering the split is the first or the last, a new
   * interval with the same step is inserted. Otherwise, the interval is splitted in two, creating a new interval.
   *
   * @param lv         [[LabeledVector]] with a instance
   * @param h          [[Histogram]] storing all internal data Structures
   * @param step       the desired step
   * @param initElems  How many elements are needed before trying to split
   * @param alpha      Threshold to launch split process
   * @param totalCount Number of instances seen so far
   * @return An [[Histogram]] updated with the new information
   */
  private[this] def updateL1(
    lv: LabeledVector,
    h: Histogram,
    step: Double,
    initElems: Int,
    alpha: Double,
    totalCount: Int): Histogram = {
    lv.vector.foldLeft(h) {
      case (z, (i, x)) ⇒
        val k =
          if (x <= z.cuts(i, 0))
            0
          else if (x > z.cuts(i, z.nColumns(i) - 1))
            z.nColumns(i) - 1 // TODO, Watch for split process
          else {
            // TODO, convert to functional
            var k = Math.ceil(x - z.cuts(i, 0) / step).toInt
            while (x <= z.cuts(i, k - 1)) k -= 1
            while (x > z.cuts(i, k)) k += 1
            k
          }
        z.updateCounts(i, k, z.counts(i, k) + 1)
        z.updateClassDistribL1(i, k, lv.label.toInt)

        // Launch the Split process
        val p = z.counts(i, k) / totalCount

        if (totalCount > initElems &&
          p > alpha) {
          val middle = h.counts(i, k) / 2d
          h.updateCounts(i, k, middle)
          val classDist = h.classDistribL1(i, k)
          val halfDist = classDist.mapValues(_ / 2d)
          h.updateClassDistribL1(i, k, halfDist)

          if (k == 0) {
            h.prependCut(i, h.cuts(i, 0) - step)
            h.prependCounts(i, middle)
            h.prependClassDistribL1(i, halfDist)
          } else if (k >= h.nColumns(i) - 1) {
            val lastBreak = h.nColumns(i) - 1
            h.appendCut(i, h.cuts(i, lastBreak) + step)
            h.appendCounts(i, middle)
            h.appendClassDistL1(i, halfDist)
          } else {
            val splitted = (h.cuts(i, k) + h.cuts(i, k + 1)) / 2d
            h.addCuts(i, k + 1, splitted)
            h.addCounts(i, k + 1, middle)
            h.addClassDistribL1(i, k, halfDist)
          }
        }
        z
    }
  }

  /**
   * In layer 2 the intervals created in layer 1 are merged to reduce its size
   *
   * @param lv [[LabeledVector]] instance
   * @param h  [[Histogram]] with all information of counts, distributions and intervals
   * @return An updated [[Histogram]]
   */
  private[this] def updateL2(
    lv: LabeledVector,
    h: Histogram): Histogram = {
    h.clearCutsL2(h.cutsSize)

    var newH = h

    for (i ← (lv.vector.size - 1) to 0 by -1) {
      val attrCuts = subSetCuts(i, 0, newH.nColumns(i), newH)

      attrCuts match {
        case Some(c) ⇒
          val lastPoint = newH.cuts(i, newH.nColumns(i) - 1)
          if (lastPoint != c.last) {
            newH.addNewCutsL2(i, c.size + 1)
            newH.updateCutsL2(i, c.to[ArrayBuffer])
            newH.appendCutL2(i, lastPoint)
          } else {
            newH.addNewCutsL2(i, c.size)
            newH.updateCutsL2(i, c.to[ArrayBuffer])
          }
        case None ⇒
          newH.addNewCutsL2(i, newH.nColumns(i))
          // Copy cuts from L1 to L2
          newH.updateCutsL2(i, newH.cuts(i))
      }
      newH = updateDistributionsL2(i, newH.cutsL2(i))(newH)
    }
    newH
  }

  /**
   * Updates the distributions in layer 2 in order to reduce the number of bins for each attribute
   *
   * @param attr      Index of the attribute to which update its distributions
   * @param newPoints Sequence of updated cutpoints to merge intervals
   * @param h         [[Histogram]] holding all the information
   * @return An updated [[Histogram]]
   */
  private[this] def updateDistributionsL2(attr: Int, newPoints: Seq[Double])(h: Histogram): Histogram = {
    val newDistribL2 = ArrayBuffer.empty[Map[Int, Double]]
    val interval = collection.mutable.Map.empty[Int, Double]
    var j = 0
    var cont = true
    for (i ← newPoints.indices) {
      //      val r = h.cuts(attr).takeWhile(_ <= newPoints(i)).size
      while (cont && h.cuts(attr, j) <= newPoints(i)) {
        val old = h.classDistribL1(attr, j)
        for (key ← old.keySet) {
          if (interval.contains(key))
            interval(key) = interval(key) + old(key)
          else
            interval(key) = old(key)
        }
        j += 1
        if (j >= h.cuts(attr).size) cont = false
      }
      newDistribL2 append interval.toMap
      interval.clear
    }
    h.updateClassDistribL2(attr, newDistribL2)
    h
  }

  /**
   * Recursive Entropy Discretization Algorithm.
   *
   * An implemantation of Fayyad & and Irani <a href="http://yaroslavvb.com/papers/fayyad-discretization.pdf">ref.</a>
   *
   * This algorithm uses the class information entropy of candidate partitions to select the boundaries for
   * discretization. It starts finding a single threshold that minimizes the entropy function over all possible
   * cut-points; it is then recursively applied to both of the partitions. The stopping criteria uses the
   * Minimum Description Length Principle <a href="https://en.wikipedia.org/wiki/Minimum_description_length">ref.</a>.
   *
   * @param index  Index for the current attribute
   * @param first  Index for the first sets of intervals to examine
   * @param last   Index for the last sets of intervals to examine
   * @param h      [[Histogram]] with all the information
   * @return The new calculated cutpoints
   */
  private[this] def subSetCuts(index: Int, first: Int, last: Int, h: Histogram): Option[Seq[Double]] = {

    if ((last - first) < 2)
      None
    else {
      // Greatest class observed till the moment
      val priorCounts = h.classCounts(index, first, last)
      val priorH = InformationTheory.entropy(priorCounts.last)
      // Find best Entropy
      val (_, bestH, bestIndex, nCuts, bestCounts) = h.distribMatrixL1(index)
        .zipWithIndex
        .slice(first, last - 1)
        ./:(
          priorCounts.map(_.toArray).toArray, // Counts
          priorH, // BestEntropy
          0, // BestIndex
          0, // NCuts
          Vector.empty[Vector[Double]]) { // BestCounts
            case (z, (m, i)) ⇒
              import scala.collection.JavaConversions._
              for (entry ← m.entrySet) {
                z._1(0)(entry.getKey) += entry.getValue
                z._1(1)(entry.getKey) -= entry.getValue
              }

              val currentCounts = z._1.map(_.toVector).toVector
              val contingencyMatrix = z._1.map(_.toSeq).toSeq
              val currH = InformationTheory.entropyConditionedOnRows(contingencyMatrix)

              if (currH < z._2)
                (z._1, currH, i, i + 1, currentCounts)
              else
                (z._1, z._2, z._3, i + 1, z._5)
          }

      // Check if gain is zero
      if ((priorH - bestH) <= 0)
        None
      else {
        // Check if split is accepted
        if (InformationTheory.FayyadAndIranisMDL(
          priorCounts.last,
          bestCounts,
          priorCounts.last.sum,
          nCuts)) {
          // Select splitted points for the left and right subsets
          val left = subSetCuts(index, first, bestIndex + 1, h)
          val right = subSetCuts(index, bestIndex + 1, last, h)

          // Merge process
          val bestCut = h.cuts(index, bestIndex)

          val cutpoints = (left, right) match {
            case (None, None) ⇒
              Seq(bestCut)
            case (Some(l), None) ⇒
              l :+ bestCut
            case (None, Some(r)) ⇒
              bestCut +: r
            case (Some(l), Some(r)) ⇒
              (l :+ bestCut) ++ r
          }
          Some(cutpoints)

        } else
          None
      }
    }
  }
}

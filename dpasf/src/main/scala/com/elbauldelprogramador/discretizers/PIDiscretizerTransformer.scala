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
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import weka.core.ContingencyTables

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

  private[PIDiscretizerTransformer] var metricsOption: Option[DataSet[Histogram]] = None
  private[PIDiscretizerTransformer] lazy val step = (parameters(Max) - parameters(Min)) / parameters(L1InitialBins).toDouble

  // TODO docs
  def setUpdateExamples(l2Updates: Int): PIDiscretizerTransformer = {
    parameters add (L2UpdateExamples, l2Updates)
    this
  }

  // TODO docs
  def setL1Bins(bins: Int): PIDiscretizerTransformer = {
    parameters add (L1InitialBins, bins)
    this
  }

  // TODO docs
  def setInitElements(n: Int): PIDiscretizerTransformer = {
    parameters add (InitialElements, n)
    this
  }

  // TODO docs
  def setAlpha(alpha: Double): PIDiscretizerTransformer = {
    parameters add (Alpha, alpha)
    this
  }

  // TODO docs
  def setMin(min: Int): PIDiscretizerTransformer = {
    parameters add (Min, min)
    this
  }

  // TODO docs
  def setMax(max: Int): PIDiscretizerTransformer = {
    parameters add (Max, max)
    this
  }
}

object PIDiscretizerTransformer {

  private[this] val log = LoggerFactory.getLogger(this.getClass)

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

  // TODO doc
  implicit val fitNoOp = new FitOperation[PIDiscretizerTransformer, LabeledVector] {
    override def fit(
      instance: PIDiscretizerTransformer,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector]): Unit = {

      val resultingParameters = instance.parameters ++ fitParameters
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

        //         Update Layer 2 if neccesary
        val updatedL2 = if (m1._3 % l2updateExamples == 0) {
          updateL2(m1._1, updatedL1)
        } else updatedL1

        (m2._1, updatedL2, m1._3 + 1)
      }.map(_._2)

      //      instance.metricsOption = Some(metric)

      log.info(s"H: ${metric.print}")
    }
  }

  // TODO doc
  implicit val transformLabeledIDADiscretizer = new TransformDataSetOperation[PIDiscretizerTransformer, LabeledVector, LabeledVector] {
    override def transformDataSet(
      instance: PIDiscretizerTransformer,
      transformParameters: ParameterMap,
      input: DataSet[LabeledVector]): DataSet[LabeledVector] = {
      val resultingParameters = instance.parameters ++ transformParameters

      instance.metricsOption match {
        case Some(m) ⇒
          m.print
          input.map(l ⇒ LabeledVector(l.label + 1, l.vector))
        case None ⇒ input
      }
    }
  }

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

  private[this] def updateL2(
    lv: LabeledVector,
    h: Histogram): Histogram = {

    h.clearCutsL2

    for (i ← (lv.vector.size - 1) to 0 by -1) {
      val attrCuts = subSetCuts(i, 0, h.nColumns(i), h)

      attrCuts match {
        case Some(c) ⇒
          val lastPoint = h.cuts(i, h.nColumns(i) - 1)
          h.clearCutsL2(i)
          if (lastPoint != c(c.size - 1)) {
            h.updateCutsL2(i, c.to[ArrayBuffer])
            h.updateCutsL2(i, h.nColumns(i) - 1, lastPoint)
          } else {
            h.updateCutsL2(i, c.to[ArrayBuffer])
          }
        case None ⇒
          h.clearCutsL2(i)
          for (j ← 0 until h.nColumns(i)) {
            h.updateCutsL2(i, j, h.cuts(i, j))
          }
      }
      updateDistributionsL2(i, h.cutsL2(i))(h)
    }

    //val cuts = lv.vector.foldRight(Seq.empty[Double]) {
    //  case ((i, _), z) =>
    //    val attrCuts = subSetCuts(i, 0, h.nColumns(i), h)
    //
    //    attrCuts match {
    //      case Some(c) => c
    //      case None =>
    //        h.clearCutsL2(i)
    //        for (j <- 0 until h.nColumns(i)) {
    //          h.updateCutsL2(i, j, h.cuts(i, j))
    //        }
    //    }
    //}
    h

  }

  private[this] def updateDistributionsL2(attr: Int, newPoints: Seq[Double])(h: Histogram): Histogram = {
    ???
  }

  private[this] def subSetCuts(index: Int, first: Int, last: Int, h: Histogram): Option[Seq[Double]] = {

    if ((last - first) < 2) {
      log.error("(last - first) < 2")
      None
    } else {
      // Greatest class observed till the moment
      val nClasses = h.greatestClass(index, first, last)
      val priorCounts = h.classCounts(index, first, last)
      val priorMatrix = Seq.tabulate(nClasses)(priorCounts.getOrElse(_, 0d))
      val priorH = InformationTheory.entropy(priorCounts.values.toVector)

      // Find best Entropy
      val (counts1, bestH, bestIndex, nCuts) = h.distribMatrixL1(index)
        .zipWithIndex
        .slice(first, last - 1) // last - 1
        //  (counts,                 counts,      bestEntropy, bestIndex, nCutpoints)
        ./:((Array.tabulate(2)(i ⇒ if (i == 1) priorCounts.values.toArray else Array.fill(nClasses)(0d)), priorH, 0, 0)) {
          case (z, (m, i)) ⇒
            import scala.collection.JavaConversions._
            for (entry ← m.entrySet) {
              z._1(0)(entry.getKey) += entry.getValue
              z._1(1)(entry.getKey) -= entry.getValue
            }
            //          val counts = m./:(z) { (z, x) =>
            //            val z1Updated = z._1.updated(x._1, z._1(x._1) + x._2)
            //            val z2Updated = z._2.updated(x._1, z._2(x._1) - x._2)
            //
            //            (z1Updated, z2Updated, z._3, z._4, z._5)
            //          }
            val currentCut = h.cuts(index, i)
            val contingencyMatrix = z._1.map(_.toSeq).toSeq
            val currH = InformationTheory.entropyConditionedOnRows(contingencyMatrix)

            if (currH < z._2)
              (z._1, currH, i, i + 1)
            else
              (z._1, z._2, z._3, i + 1)
        }
      // (List(0.0, 0.0),List(494.0, 506.0),0.9998961234639354,0,0)
      import weka.core.ContingencyTables
      import java.util
      // Find best entropy.// Find best entropy.

      //      val bestCounts = new Array[Array[Double]](2, numClasses)
      var bestEntropy = priorH
      val bestCounts = Array.fill(2)(Array.fill(nClasses)(0d))
      val counts = Array.tabulate(2)(i ⇒ if (i == 1) priorCounts.values.toArray else Array.fill(nClasses)(0d))
      var i = first
      var bestCutpoint = Map.empty[Int, Double]
      var bestI = 0
      var numCutPoints = 0

      while ({
        i < (last - 1)
      }) {
        val classDist = h.distribMatrixL1(index)(i)
        import scala.collection.JavaConversions._
        for (entry ← classDist.entrySet) {
          counts(0)(entry.getKey) += entry.getValue
          counts(1)(entry.getKey) -= entry.getValue
        }
        val currCut = h.distribMatrixL1(index)(i)
        val currH = InformationTheory.entropyConditionedOnRows(counts.map(_.toSeq).toSeq) //ContingencyTables.entropyConditionedOnRows(counts)
        if (currH < bestEntropy) {
          bestCutpoint = currCut
          bestEntropy = currH
          bestI = i
          System.arraycopy(counts(0), 0, bestCounts(0), 0, nClasses)
          System.arraycopy(counts(1), 0, bestCounts(1), 0, nClasses)
        }
        numCutPoints += 1

        {
          i += 1; i - 1
        }
      }

      assert(bestI == bestIndex)

      // Check if gain is zero
      if ((priorH - bestH) <= 0) {
        log.error("(priorH - bestH) <= 0")
        None
      } else {
        log.error("Else gain")
        // Check if split is accepted
        // https://github.com/tmadl/sklearn-expertsys/blob/master/Discretization/MDLP.py
        if (InformationTheory.FayyadAndIranisMDL(
          priorCounts,
          counts1.map(_.toSeq).toSeq,
          priorMatrix.sum,
          nCuts)) {
          log.error("FAyyad meets")
          // Select splitted points for the left and right subsets
          log.error(s"Calling left /w (first, best+1) = ($first, ${bestIndex + 1}")
          val left = subSetCuts(index, first, bestIndex + 1, h)
          log.error(s"Calling right /w (best+1, last) = (${bestIndex + 1}, $last")
          val right = subSetCuts(index, bestIndex + 1, last, h)
          log.error(s"Left & right: $left, $right")

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
          log.error(s"FAyyad meets for this cut: $cutpoints")
          Some(cutpoints)

        } else {
          log.error("FAyyad does not meets")
          None
        }
      }
    }
  }
}

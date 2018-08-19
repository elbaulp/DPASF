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
import com.elbauldelprogramador.utils.FlinkUtils
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}
import org.slf4j.LoggerFactory

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

      val metric = input.map { x =>
        // (instance, histrogram totalCount)
        (x, Histogram(nAttrs, l1InitialBins, min, instance.step), 1)
      }.reduce { (m1, m2) =>
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
        case Some(m) =>
          m.print
          input.map(l => LabeledVector(l.label + 1, l.vector))
        case None => input
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
      case (z, (i, x)) =>
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

      val cuts = lv.vector.foldRight(h) {
        case ((i, x), z) =>
          val attrCuts = subSetCuts(i, 0, z.nColumns(i), z)
          z
      }
      cuts

    }

    private[this] def subSetCuts(index: Int, first: Int, last: Int, h: Histogram): Array[ArrayBuffer[Double]] = {
      require((last - first) >= 2, s"($last - $first) >= 2")

      // Greatest class observed till the moment
      val nClasses = h.greatestClass(index, first, last)

      Array.empty[ArrayBuffer[Double]]
    }
}

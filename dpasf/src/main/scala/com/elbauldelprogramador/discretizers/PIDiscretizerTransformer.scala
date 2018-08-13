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
import org.apache.flink.ml.common.{ LabeledVector, Parameter, ParameterMap }
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }
import org.slf4j.LoggerFactory

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

  private[this] var metricsOption: Option[DataSet[Histogram]] = None
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
  def setNumAttr(alpha: Double): PIDiscretizerTransformer = {
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

      val r = input.map { x =>
        (x, Histogram(nAttrs, l1InitialBins, min, instance.step))
      }.reduce { (m1, m2) =>
        // Update Layer 1
        val updated = updateL1(m1._1, m1._2)(instance)

        (m2._1, updated)
      }.map(_._2)
      log.info(s"H: ${r.print}")
    }
  }

  // TODO doc
  implicit val transformLabeledIDADiscretizer = new TransformDataSetOperation[PIDiscretizerTransformer, LabeledVector, LabeledVector] {
    override def transformDataSet(
      instance: PIDiscretizerTransformer,
      transformParameters: ParameterMap,
      input: DataSet[LabeledVector]): DataSet[LabeledVector] = {
      val resultingParameters = instance.parameters ++ transformParameters
      input.print

      input
    }
  }

  private[this] def updateL1(lv: LabeledVector, h: Histogram)(instance: PIDiscretizerTransformer): Histogram = {
    lv.vector.foldLeft(h) {
      case (z, (i, x)) =>
        val k =
          if (x <= z.cuts(i, 0))
            0
          else if (x > z.cuts(i, z.nCols - 1))
            z.nCols - 1 // TODO, Watch for split process
          else {
            // TODO, convert to functional
            var k = Math.ceil(x - z.cuts(i, 0) / instance.step).toInt
            while (x <= z.cuts(i, k - 1)) k -= 1
            while (x > z.cuts(i, k)) k += 1
            k
          }
        z.updateCounts(i, k, z.counts(i, k) + 1)
        z.updateClassDistrib(i, k, lv.label.toInt)
        z
      // Launch the Split process
    }
  }
}

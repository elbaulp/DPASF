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

import com.elbauldelprogramador.datastructures.IntervalHeapWrapper
import com.elbauldelprogramador.utils.FlinkUtils
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}
import org.slf4j.LoggerFactory

/**
 * Incremental Discretization Algorithm
 */
class IDADiscretizerTransformer extends Transformer[IDADiscretizerTransformer] {

  import IDADiscretizerTransformer._

  private[discretizers] var cuts: Option[DataSet[Vector[IntervalHeapWrapper]]] = None
  //private[this] lazy val randomReservoir = SamplingUtils.reservoirSample((1 to parameters(SampleSize)).toList.iterator, 1)

  /**
   * Sets the numbers of bin for the discretization.
   *
   * @param bins Number of bins
   * @return itself
   */
  def setBins(bins: Int): IDADiscretizerTransformer = {
    parameters add (Bins, bins)
    this
  }

  /**
   * Sets the sample size to maintain.
   *
   * @param sSize Sample size to use.
   * @return itself.
   */
  def setSampleSize(sSize: Int): IDADiscretizerTransformer = {
    parameters add (SampleSize, sSize)
    this
  }
}

object IDADiscretizerTransformer {

    private[this] val log = LoggerFactory.getLogger(this.getClass)

  // ========================================== Parameters =========================================
  private[IDADiscretizerTransformer] case object Bins extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(5)
  }

  private[IDADiscretizerTransformer] case object SampleSize extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(1000)
  }

  // ========================================== Factory methods ====================================
  def apply(): IDADiscretizerTransformer = new IDADiscretizerTransformer

  // ========================================== Operations =========================================

  /**
   * [[IDADiscretizerTransformer]] does not need a fitting phase
   */
  implicit val fitLabeledVectorIDADiscretizer = new FitOperation[IDADiscretizerTransformer, LabeledVector] {
    override def fit(
      instance: IDADiscretizerTransformer,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector]): Unit = {

      val resultingParameters = instance.parameters ++ fitParameters
      val bins = resultingParameters(Bins)

      val nAttrs = FlinkUtils.numAttrs(input)
      val v = Vector.tabulate(nAttrs)(i ⇒ new IntervalHeapWrapper(bins, i))

      val discretized = computeCuts(input, bins, nAttrs, v)
      val cuts =
        discretized.map(_._2)
          .name("Get V")
          .reduce((_, b) ⇒ b)
      instance.cuts = Some(cuts)
    }
  }

  /**
   * [[IDADiscretizerTransformer]] that discretizes the [[DataSet]], which is of [[LabeledVector]] using the
   * IDA algorithm with the given parameters (Number of bins in which to computeCuts)
   */
  implicit val transformLabeledIDADiscretizer = new TransformDataSetOperation[IDADiscretizerTransformer, LabeledVector, LabeledVector] {
    override def transformDataSet(
      instance: IDADiscretizerTransformer,
      transformParameters: ParameterMap,
      input: DataSet[LabeledVector]): DataSet[LabeledVector] = {

      instance.cuts match {
        case Some(v) ⇒
          discretize(input, v)
        case None ⇒ throw new RuntimeException("The IDADiscretizer has not been transformed. " +
          "This is necessary to retrieve the cutpoints for future discretizations.")
      }
    }
  }

  /**
   * Discretize the input [[DataSet]] with its corresponding bin
   *
   * @param input [[DataSet]] to Discretize
   * @param cuts   The computed cutpoints from the fit phase
   * @return The input [[DataSet]] discretized
   */
  private[this] def discretize(input: DataSet[LabeledVector], cuts: DataSet[Vector[IntervalHeapWrapper]]): DataSet[LabeledVector] =
    input.mapWithBcVariable(cuts) { (lv, c) ⇒
      val attrs = lv.vector.map(_._2).toSeq.zipWithIndex
      val bins = attrs map {
        case (v, i) ⇒
          c(i).getBin(v)
      }
      LabeledVector(lv.label, DenseVector(bins.toArray))
    }

  /**
   * Discretize the entire [[DataSet]]
   *
   * @param input  [[DataSet]] to computeCuts
   * @param bins   Number of bins to use in the discretization
   * @param nAttrs Total number of attributes for the [[DataSet]]
   * @param V      [[IntervalHeapWrapper]] used to compute discretization
   * @return The discretized [[DataSet]]  in which each value of the [[LabeledVector]]
   *         corresponds with the bin the value was discretized to.
   */
  private[this] def computeCuts(
    input: DataSet[LabeledVector],
    bins: Int,
    nAttrs: Int,
    V: Vector[IntervalHeapWrapper]): DataSet[(LabeledVector, Vector[IntervalHeapWrapper])] = input.map { lv ⇒
    val attrs = lv.vector.map(_._2)
    val label = lv.label
    // TODO: Check for missing values
    attrs
      .zipWithIndex
      .foldLeft((LabeledVector(label, DenseVector.init(attrs size, -1000)), V)) {
        case ((vec, v), (x, i)) ⇒
          //          if (V(i).getNbSamples < s) {
          v(i) insertValue x // insert
          vec.vector.update(i, v(i) getBin x)
          (vec, v)
      }
  } name "Discretize"
}

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
import org.apache.flink.ml.common.{ LabeledVector, Parameter, ParameterMap }
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }

/**
 * Incremental Discretization Algorithm
 */
class IDADiscretizerTransformer extends Transformer[IDADiscretizerTransformer] {

  import IDADiscretizerTransformer._

  private[discretizers] var cuts: Option[Vector[Vector[Double]]] = None
  //  private[discretizers] var V: Option[Vector[IntervalHeapWrapper]] = None
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

  /**
   * Use a previously computed discretization to discretize a [[DataSet]] with the given
   * cutpoints.
   *
   * @param data The new [[DataSet]] to discretize
   * @return The input [[DataSet]] discretized with the same cutpoints as the previous discretization
   */
  def discretizeWith(data: DataSet[LabeledVector]): DataSet[LabeledVector] = {
    cuts match {
      case Some(c) ⇒
        data.map { l ⇒
          val attrs = l.vector map (_._2) toSeq
          val dattrs = assignDiscreteValue(attrs, c)
          LabeledVector(l.label, DenseVector(dattrs.toArray))
        } name "DiscretizeWith"
      case None ⇒ throw new RuntimeException("The IDADiscretizer has not been transformed. " +
        "This is necessary to retrieve the cutpoints for future discretizations.")
    }
  }
}

object IDADiscretizerTransformer {

  //  private[this] val log = LoggerFactory.getLogger(this.getClass)

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
  implicit val fitNoOp = new FitOperation[IDADiscretizerTransformer, LabeledVector] {
    override def fit(
      instance: IDADiscretizerTransformer,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector]): Unit = ()
  }

  /**
   * [[IDADiscretizerTransformer]] that discretizes the [[DataSet]], which is of [[LabeledVector]] using the
   * IDA algorithm with the given parameters (Number of bins in which to discretize)
   */
  implicit val transformLabeledIDADiscretizer = new TransformDataSetOperation[IDADiscretizerTransformer, LabeledVector, LabeledVector] {
    override def transformDataSet(
      instance: IDADiscretizerTransformer,
      transformParameters: ParameterMap,
      input: DataSet[LabeledVector]): DataSet[LabeledVector] = {
      val resultingParameters = instance.parameters ++ transformParameters

      val bins = resultingParameters(Bins)

      val nAttrs = FlinkUtils.numAttrs(input)
      val v = Vector.tabulate(nAttrs)(i ⇒ new IntervalHeapWrapper(bins, i))

      val discretized = discretize(input, bins, nAttrs, v)
      instance.cuts = Some(cutPoints(input, v))

      discretized
    }
  }

  /**
   * Map a value to its corresponding bin
   *
   * @param vs   LabeledVector attributes
   * @param cuts The cutpoints for each attribute and its bins
   * @return The attributes assigned to its bins
   */
  private[IDADiscretizerTransformer] def assignDiscreteValue(
    vs: Seq[Double],
    cuts: Seq[Seq[Double]]): Seq[Double] =
    vs.zipWithIndex map {
      case (v, i) ⇒
        (cuts(i) indexWhere (v <= _)).toDouble
    }

  /**
   * Return the cutpoints for the current discretization
   *
   * @param data The Dataset to obtain the cutpoints from.
   * @param V    [[IntervalHeapWrapper]] used to compute discretization
   * @return Cutpoints for each bin
   */
  private[this] def cutPoints(
    data: DataSet[LabeledVector],
    V: Vector[IntervalHeapWrapper]): Vector[Vector[Double]] =
    data.map { x ⇒
      val attrs = x.vector map (_._2)
      attrs
        .zipWithIndex
        .foldLeft(V) {
          case (iv, (v, i)) ⇒
            iv(i) insertValue v
            iv
        }
    }.collect
      .last map (_.getBoundaries.toVector)

  /**
   * Discretize the entire [[DataSet]]
   *
   * @param input  [[DataSet]] to discretize
   * @param bins   Number of bins to use in the discretization
   * @param nAttrs Total number of attributes for the [[DataSet]]
   * @param V      [[IntervalHeapWrapper]] used to compute discretization
   * @return The discretized [[DataSet]]  in which each value of the [[LabeledVector]]
   *         corresponds with the bin the value was discretized to.
   */
  private[this] def discretize(
    input: DataSet[LabeledVector],
    bins: Int,
    nAttrs: Int,
    V: Vector[IntervalHeapWrapper]): DataSet[LabeledVector] = input.map { v ⇒
    val attrs = v.vector.map(_._2)
    val label = v.label
    // TODO: Check for missing values
    attrs
      .zipWithIndex
      .foldLeft(LabeledVector(label, DenseVector.init(attrs size, -1000))) {
        case (lv, (x, i)) ⇒
          //          if (V(i).getNbSamples < s) {
          V(i) insertValue x // insert
          lv.vector.update(i, V(i) getBin x)
          lv
      }
  } name "Discretize"
}

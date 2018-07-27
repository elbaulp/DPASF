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

import com.elbauldelprogramador.discretizers.LOFDiscretizer
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}
import org.slf4j.LoggerFactory
import sun.rmi.runtime.Log.LoggerLogFactory

import scala.collection.immutable.{Queue, TreeMap}
import scala.collection.mutable
//import scala.collection.mutable.MutableList
//import scala.collection.mutable.Vector

/**
 * Local Online Fusion Discretizer (LOFD)
 *
 *
 * An online discretizer that fuse intervals locally using a measure based on quadratic entropy.
 *
 * Associated Paper:
 *
 * <a href="https://doi.org/10.1016/j.future.2018.03.008">
 *    Online entropy-based discretization for data streaming classification
 * </a>
 */
class LOFDiscretizerTransformer extends Transformer[LOFDiscretizerTransformer] {

  import LOFDiscretizerTransformer._

  private[discretizers] var cuts: Option[Array[Array[Double]]] = None

  /**
   * Sets the Alpha parameter
   *
   * @return [[LOFDiscretizerTransformer]]
   */
  def setAlpha(alpha: Double): LOFDiscretizerTransformer = {
    parameters add (Alpha, alpha)
    this
  }

  /**
   * Sets the lambda parameter
   *
   * @return [[LOFDiscretizerTransformer]]
   */
  def setLambda(lambda: Double): LOFDiscretizerTransformer = {
    parameters add (Lambda, lambda)
    this
  }

  /**
   * Sets the number of instances before initializing intervals
   *
   * @return [[LOFDiscretizerTransformer]]
   */
  def setInitTh(n: Int): LOFDiscretizerTransformer = {
    parameters add (InitTH, n)
    this
  }

  /**
   * Sets the maximum number of elements in interval histograms
   *
   * @return [[LOFDiscretizerTransformer]]
   */
  def setMaxHist(n: Int): LOFDiscretizerTransformer = {
    parameters add (MaxHist, n)
    this
  }

  /**
   * Sets the Decimals parameter
   *
   * @return [[LOFDiscretizerTransformer]]
   */
  def setDecimals(decimals: Int): LOFDiscretizerTransformer = {
    parameters add (Decimals, decimals)
    this
  }

  /**
   * Sets maximun number of labels parameter
   *
   * @return [[LOFDiscretizerTransformer]]
   */
  def setMaxLabels(maxLabels: Int): LOFDiscretizerTransformer = {
    parameters add (MaxLabels, maxLabels)
    this
  }

  /**
   * Sets if LOFD should provide Likelihood values
   *
   * @return [[LOFDiscretizerTransformer]]
   */
  def setProvideProb(probs: Boolean): LOFDiscretizerTransformer = {
    parameters add (ProvideProb, probs)
    this
  }
}

object LOFDiscretizerTransformer {

  private[this] val log = LoggerFactory.getLogger(this.getClass)

  // ========================================== Parameters =========================================
  private[LOFDiscretizerTransformer] case object Alpha extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(.5)
  }

  private[LOFDiscretizerTransformer] case object Lambda extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(.5)
  }

  private[LOFDiscretizerTransformer] case object InitTH extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(5000)
  }

  private[LOFDiscretizerTransformer] case object MaxHist extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(10000)
  }

  private[LOFDiscretizerTransformer] case object Decimals extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(3)
  }

  private[LOFDiscretizerTransformer] case object MaxLabels extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(1000)
  }

  private[LOFDiscretizerTransformer] case object ProvideProb extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = Some(true)
  }

  // ========================================== Factory methods ====================================
  def apply(): LOFDiscretizerTransformer = new LOFDiscretizerTransformer

  // ========================================== Operations =========================================

  /**
    * [[LOFDiscretizerTransformer]] does not need a fitting phase
    */
  implicit val fitNoOp = new FitOperation[LOFDiscretizerTransformer, LabeledVector] {
    override def fit(
      instance: LOFDiscretizerTransformer,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector]): Unit = ()



//      val allIntervals = Vector.tabulate(nAttrs)(_ => TreeMap.empty[Double, Int]) // TODO INTERVAL
//      val cutpoints = Vector.tabulate(nAttrs)(_ => Vector.empty[Double])
//      val labels = Vector.tabulate(nAttrs)(_ => Vector.empty[String])
//      val elemQ = Vector.tabulate(nAttrs)(_ => mutable.MutableList.empty[(Double, Byte)])
//      val contLabels = Vector.fill(nAttrs)(maxLabels)
//      val classByAtt = mutable.Seq.fill(nAttrs, nClasses)(0)
//      val labelsToUse = Vector.tabulate(nAttrs)(_ => Queue((1 to maxLabels + 1): _*))
//
//      var totalCount = 0
//
//      // Create Scheme
//      val r = input.map { v =>
//        totalCount += 1
//        // Count how many elements there are per class
//        v.vector.foreach {
//          case (i, _) =>
//            classByAtt(i)(v.label.toInt) = classByAtt(i)(v.label.toInt) + 1
//        }
//
//        if (totalCount >= initTh){
//          v.vector.foreach {
//            case (i, _) =>
//              insertExample(i, v, resultingParameters)
//          }
//        }
//
//      }

  }

  /**
   * Insert a new example in the discretization scheme. If it is a boundary point,
   * it is incorporated and a local fusion process is launched using this interval and
   * the surrounding ones. If not, the point just feed up the intervals.
    *
   * @param att Attribute index
   * @param v   An incoming example
   */
  private[this] def insertExample(att: Int, v: LabeledVector, params: ParameterMap): Unit = {
    val cls = v.label.toInt
    val value = getInstanceValue(att, params(Decimals))

    // Get the ceiling Interval for the given value
    ???
  }

  /**
   * Transform a point by rounding it according to the number of decimals
   * defined as input parameter.
    *
   * @param value Value to transform
   * @return Rounded value.
   */
  private[this] def getInstanceValue(value: Double, decimals: Int): Double = {
    if(decimals > 0) {
      val mult = Math.pow(10, decimals)
      Math.round(value * mult) / mult
    }
    value
  }

  implicit val transformDataSetLabeledVectorsInfoGain = {
    new TransformDataSetOperation[LOFDiscretizerTransformer, LabeledVector, LabeledVector] {
      override def transformDataSet(
        instance: LOFDiscretizerTransformer,
        transformParameters: ParameterMap,
        input: DataSet[LabeledVector]): DataSet[LabeledVector] = {

        val resultingParameters = instance.parameters ++ transformParameters
        val alpha = resultingParameters(Alpha)
        val lambda = resultingParameters(Lambda)
        val initTh = resultingParameters(InitTH)
        val maxHist = resultingParameters(MaxHist)
        val decimals = resultingParameters(Decimals)
        val maxLabels = resultingParameters(MaxLabels)
        val provideProb = resultingParameters(ProvideProb)

        // Initialize variables
        // Thanks to https://stackoverflow.com/a/51497661/1612432
//        val nAttrs = input
//          // only forward first vector of each partition
//          .mapPartition(in => if (in.hasNext) Seq(in.next) else Seq())
//          // move all remaining vectors to a single partition, compute size of the first and forward it
//          .mapPartition(in => if (in.hasNext) Seq(in.next.vector.size) else Seq())
//          .setParallelism(1)
//          .collect
//          .head

        val nClasses = input.map(_.label)
          .distinct
          .count
          .toInt

        val lofd = new LOFDiscretizer(maxHist,
          initTh,
          decimals,
          maxLabels,
          nClasses)

        input.map (lofd.applyDiscretization _)
      }
    }
  }

  //  class MapWithCounter extends RichMapFunction[LabeledVector,LabeledVector] {
  //    @transient private var counter: Counter = _
  //
  //    override def open(parameters: Configuration): Unit = {
  //      counter = getRuntimeContext()
  //        .getMetricGroup()
  //        .counter("myCounter")
  //    }
  //
  //    override def map(value: LabeledVector) = {
  //      counter.inc()
  //      value
  //    }
  //  }
}

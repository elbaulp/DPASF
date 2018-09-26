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

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{ LabeledVector, Parameter, ParameterMap }
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }
import org.slf4j.LoggerFactory

import scala.collection.immutable

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

  private[discretizers] var cuts: Option[DataSet[immutable.IndexedSeq[Array[Double]]]] = None

  import LOFDiscretizerTransformer._

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

  def setNClass(n: Int): LOFDiscretizerTransformer = {
    parameters add (NCLasses, n)
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

  private[LOFDiscretizerTransformer] case object NCLasses extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(2)
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
      input: DataSet[LabeledVector]): Unit = {
      val resultingParameters = instance.parameters ++ fitParameters
      val alpha = resultingParameters(Alpha)
      val lambda = resultingParameters(Lambda)
      val initTh = resultingParameters(InitTH)
      val maxHist = resultingParameters(MaxHist)
      val decimals = resultingParameters(Decimals)
      val maxLabels = resultingParameters(MaxLabels)
      val provideProb = resultingParameters(ProvideProb)

      val nClasses = resultingParameters(NCLasses)

      val lofd = new LOFDiscretizer(
        maxHist,
        initTh,
        decimals,
        maxLabels,
        nClasses)

      val cuts = input.map { x ⇒
        val s = lofd applyDiscretization x
        for (s ← 0 until s.vector.size) yield {
          Option(lofd.getCutPoints(s))
        }
      }.name("ApplyDiscretization")
        .reduce((_, b) ⇒ b)
        .name("Reducing")
        .map(_.map(_.getOrElse(Array.empty)))
        .name("Last cuts")

      instance.cuts = Some(cuts)

    }
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

        instance.cuts match {
          case Some(c) ⇒
            input.mapWithBcVariable(c) {
              case (lv, cuts) ⇒
                val attrs = lv.vector
                val d = attrs.map {
                  case (i, v) ⇒
                    val bin = cuts(i).indexWhere(v < _)
                    if (bin == -1 && v < cuts(i).head) -2 else bin
                }
                LabeledVector(lv.label, DenseVector(d.toArray))
            }
          case None ⇒
            throw new RuntimeException("The LOFDiscretizer has not been transformed. " +
              "This is necessary to retrieve the cutpoints for future discretizations.")
        }
      }
    }
  }
}

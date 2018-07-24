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
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }

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

object  LOFDiscretizerTransformer {

  // ========================================== Parameters =========================================
  private[LOFDiscretizerTransformer] case object Alpha extends Parameter[Double]{
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
  implicit val fitLabeledVectorLOFD = new FitOperation[LOFDiscretizerTransformer, LabeledVector] {
    override def fit(instance: LOFDiscretizerTransformer,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector]): Unit = {

      val resultingParameters = instance.parameters ++ fitParameters

      ???
    }
  }

  implicit val transformDataSetLabeledVectorsInfoGain = {
    new TransformDataSetOperation[LOFDiscretizerTransformer, LabeledVector, LabeledVector] {
      override def transformDataSet(
        instance: LOFDiscretizerTransformer,
        transformParameters: ParameterMap,
        input: DataSet[LabeledVector]): DataSet[LabeledVector] = {

        val resultingParameters = instance.parameters ++ transformParameters

        ???
      }
    }
  }
}

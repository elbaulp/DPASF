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
package com.elbauldelprogramador.featureselection

import com.elbauldelprogramador.utils.{ FlinkUtils, InformationTheory }
import org.apache.flink.api.scala.{ DataSet, _ }
import org.apache.flink.ml.common.{ LabeledVector, Parameter, ParameterMap }
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Fast Correlation-Based Filter (FCBF) algorithm as described in
 * Feature Selection for High-Dimensional Data: A Fast Correlation-Based
 * Filter Solution. Yu & Liu (ICML 2003)
 */
class FCBFTransformer extends Transformer[FCBFTransformer] {

  import FCBFTransformer._

  private[featureselection] var metricsOption: Option[DataSet[Seq[Int]]] = None

  /**
   * Sets the SU threshold. A value in [0,1) used as a threshold
   * for selecting the relevant features.
   *
   * @param thresh Desired threshold
   * @return [[FCBFTransformer]]
   */
  def setThreshold(thr: Double): FCBFTransformer = {
    parameters add (Threshold, thr)
    this
  }
}

/**
 * Companion object of FCBF. Contains convenience functions and the parameter
 * type definitions of the algorithm
 */
object FCBFTransformer {

  private[this] val log = LoggerFactory.getLogger(this.getClass)

  // ====================================== Parameters =============================================
  private[FCBFTransformer] case object Threshold extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(.5)
  }

  // ==================================== Factory methods ==========================================
  def apply(): FCBFTransformer = new FCBFTransformer

  // ========================================== Operations =========================================

  implicit val fitLabeledVectorFCBF = new FitOperation[FCBFTransformer, LabeledVector] {
    override def fit(
      instance: FCBFTransformer,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector]): Unit = {

      val resultingParameters = instance.parameters ++ fitParameters
      val thr = instance.parameters(Threshold)

      val nAttrs = FlinkUtils.numAttrs(input)
      log.debug(s"Reading Dataset with $nAttrs attrs")

      // Phase 1, calculate SU (symmetrical_uncertainty) for each Feature
      // w.r.t the class (C-Correlation)
      val su = for (i ← 0 until nAttrs) yield {
        val attr = input.map(lv ⇒ LabeledVector(lv.label, DenseVector(lv.vector(i))))
        InformationTheory.symmetricalUncertainty(attr)
      }
    }
  }

  /**
   * Computes C-Correlation of each feature w.r.t the class label.
   * This value corresponds with the Symmetrical Uncertainty of each
   * feature with the label. It is computed as:
   *
   * SU(X, y) = 2 * (IG(X|Y) / (H(X) + H(Y)))
   *
   * @param input [[DataSet]] to compute SU to
   * @return Vector[Double] with SU indexed for eah attribute (v(0) corresponds with
   * SU for feature 0)
   */
  private[this] def cCorrelation(input: DataSet[LabeledVector]): Vector[Double] = {
    ???
  }

  implicit val transformDataSetLabeledVectorsFCBF = {
    new TransformDataSetOperation[FCBFTransformer, LabeledVector, LabeledVector] {
      override def transformDataSet(
        instance: FCBFTransformer,
        transformParameters: ParameterMap,
        input: DataSet[LabeledVector]): DataSet[LabeledVector] = {

        val resultingParameters = instance.parameters ++ transformParameters

        //InformationTheory.symmetricalUncertainy(input)
        ???
      }
    }
  }
}

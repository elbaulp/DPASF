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

import breeze.linalg.norm
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap, WeightVector}
import org.apache.flink.ml.math.{Breeze, DenseVector}
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}

/**
 * Online Feature Selection (OFS) using a Linear Classifier with sparse
 * projection.
 *
 * Based on the following paper:
 *
 * J. Wang, P. Zhao, S. C. H. Hoi and R. Jin, "Online Feature Selection and Its Applications,"
 * in IEEE Transactions on Knowledge and Data Engineering, vol. 26, no. 3, pp. 698-710, March 2014.
 * doi: <a href="https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6522405">https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6522405</a>
 */
class OFSGDTransformer extends Transformer[OFSGDTransformer] {

  import OFSGDTransformer._

  /**
   * Sets learning rate parameter (ETA)
   *
   * @param eta Learning rate (Deafault 0.2)
   * @return [[OFSGDTransformer]]
   */
  def setEta(eta: Double): OFSGDTransformer = {
    parameters add (Eta, eta)
    this
  }

  /**
   * Sets the regularization parameter (Lambda)
   *
   * @param lambda Regularization value (Default 0.01)
   * @return [[OFSGDTransformer]]
   */
  def setLambda(lambda: Double): OFSGDTransformer = {
    parameters add (Lambda, lambda)
    this
  }

  /**
   * Sets the number of features to select
   *
   * @param n Number of features to select
   * @return [[OFSGDTransformer]]
   */
  def setNFeature(n: Int): OFSGDTransformer = {
    parameters add (NFeatures, n)
    this
  }
}

object OFSGDTransformer {

  //    private[this] val log = LoggerFactory.getLogger(this.getClass)

  // ====================================== Parameters =============================================
  case object Eta extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(.2) //According to author's criteria
  }

  case object Lambda extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(.01)
  }

  case object NFeatures extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(10)
  }

  // ==================================== Factory methods ==========================================
  def apply(): OFSGDTransformer = new OFSGDTransformer

  // ========================================== Operations =========================================
  /**
   * [[OFSGDTransformer]] does not need a fitting phase
   */
  implicit val fitNoOp = new FitOperation[OFSGDTransformer, LabeledVector] {
    override def fit(
      instance: OFSGDTransformer,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector]): Unit = ()
  }

  /**
   * [[OFSGDTransformer]] that select the best attributes by
   * evaluating its worth through the computation of weights
   * using a linear classifier with sparse projection.
   */
  implicit val transformLabeledOSFSDG = new TransformDataSetOperation[OFSGDTransformer, LabeledVector, LabeledVector] {
    override def transformDataSet(
      instance: OFSGDTransformer,
      transformParameters: ParameterMap,
      input: DataSet[LabeledVector]): DataSet[LabeledVector] = {
      import Breeze._

      val resultingParameters = instance.parameters ++ transformParameters
      val eta = resultingParameters(Eta)
      val lambda = resultingParameters(Lambda)
      val nFeatures = resultingParameters(NFeatures)

      // TODO: Better way to compute dimensions
      val dimensionsDS = input.map(_.vector.size).reduce((_, b) => b).collect.head

      val values = Array.fill(dimensionsDS)(0.0)
      var weights = WeightVector(DenseVector(values), .0).weights.asBreeze

      val finalWeights = input.map { data =>
        val vector = data.vector.asBreeze
        val pred = vector dot weights

        if (pred * data.label <= 1) {
          vector :*= eta * data.label
          weights = weights + vector
          weights :*= math.min(1.0, 1 / (math.sqrt(lambda) * norm(weights)))

          // Truncate. Set all but the best nFeatures weights to zero
          if (weights.toArray.count(_ != 0) > nFeatures) {
            val topN = weights.toArray.zipWithIndex.sortBy(-_._1)
            for (i <- nFeatures until weights.size)
              weights(topN(i)._2) = 0
          }
        }
        weights
      }

      val indexes = finalWeights.collect
        .last
        .toArray
        .zipWithIndex.filter(_._1 != 0)

      input map { x =>
        val attrs = x.vector
        val bestF = indexes.map(x => attrs(x._2))
        LabeledVector(x.label, DenseVector(bestF))
      }
    }
  }
}

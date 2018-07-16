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

import com.elbauldelprogramador.featureselection.InformationTheory.entropy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap}
import org.apache.flink.ml.math.{BreezeVectorConverter, DenseVector, Vector}
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.reflect.ClassTag

/** Select the most important features based on its Information Gain.
  *
  * @example
  * {{{
  *               val gain = InfoGainTransformer()
  *                             .setNFeatures(2)
  *                             .setSelectNF(1)
  *               gain.fit(dataSet)
  *
  *               val transformedDS = gain.transform(dataSet)
  * }}}
  *
  * =Parameters=
  *
  * - [[com.elbauldelprogramador.featureselection.InfoGainTransformer.SelectNF]]: The number of features to select
  * - [[com.elbauldelprogramador.featureselection.InfoGainTransformer.NFeatures]]: Total number of features on the DS
  */
class InfoGainTransformer extends Transformer[InfoGainTransformer] {

  import InfoGainTransformer._

  private[featureselection] var H: Option[Double] = None
  private[featureselection] var gains: Option[Seq[Double]] = None

  /**
    * Sets the number of features to select
    *
    * @param n Number of features
    * @return [[InfoGainTransformer]]
    */
  def setSelectNF(n: Int): InfoGainTransformer = {
    parameters add(SelectNF, n)
    this
  }

  /**
    * Sets the total number of features for this
    * [[DataSet]]
    *
    * @param n Number of features
    * @return [[InfoGainTransformer]]
    */
  def setNFeatures(n: Int): InfoGainTransformer = {
    parameters add(NFeatures, n)
    this
  }
}

/**
  * Companion object of InfoGain. Contains convenience functions and the parameter
  * type definitions of the algorithm
  */
object InfoGainTransformer {

  private[this] val log = LoggerFactory.getLogger(this.getClass)

  // ====================================== Parameters =============================================
  case object SelectNF extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(10)
  }

  case object NFeatures extends Parameter[Int] {
    val defaultValue: Option[Int] = None
  }

  // ==================================== Factory methods ==========================================
  def apply(): InfoGainTransformer = new InfoGainTransformer

  // ========================================== Operations =========================================

  /** Trains the [[InfoGainTransformer]] by computing the total entropy of the Data, which is of type
    * [[LabeledVector]] and then calculating the Information Gain for each attribute.
    *
    * The InfoGain is then used in the transformation to select the attributes with most InfoGain.
    */
  implicit val fitLabeledVectorInfoGain = new FitOperation[InfoGainTransformer, LabeledVector] {
    override def fit(instance: InfoGainTransformer, fitParameters: ParameterMap, input: DataSet[LabeledVector]): Unit = {

      val resultingParameters = instance.parameters ++ fitParameters
      val selectNF = resultingParameters(SelectNF)
      val nf = resultingParameters(NFeatures)

      val (h, gains) = computeInfoGain(input, selectNF, nf)

      instance.H = Some(h)
      instance.gains = Some(gains)
    }
  }

  implicit def fitVectorInfoGain[T <: Vector] = new FitOperation[InfoGainTransformer, T] {
    override def fit(instance: InfoGainTransformer, fitParameters: ParameterMap, input: DataSet[T]): Unit =
      input
  }

  /** [[TransformDataSetOperation]] which select the top N attributes specified by parameters from the DataSet,
    * which is of type [[LabeledVector]].
    *
    * @return [[TransformDataSetOperation]] The DataSet with the best attributes selected
    */
  implicit val transformDataSetLabeledVectorsInfoGain = {
    new TransformDataSetOperation[InfoGainTransformer, LabeledVector, LabeledVector] {
      override def transformDataSet(
                                     instance: InfoGainTransformer,
                                     transformParameters: ParameterMap,
                                     input: DataSet[LabeledVector]): DataSet[LabeledVector] = {

        val resultingParameters = instance.parameters ++ transformParameters
        val selectNF = resultingParameters(SelectNF)
        val nf = resultingParameters(NFeatures)

        require(selectNF < nf, "Features to select must be less than total features")

        instance.gains match {
          case Some(gains) =>
            val topf = gains.sortWith(_ > _).take(selectNF)
            val indexes = topf.map(gains.indexOf(_))
            log.info(s"Selecting features $indexes")
            input.map { x =>
              val attrs = x.vector
              val output = indexes.map(attrs(_))
              LabeledVector(x.label, DenseVector(output.toArray))
            }

          case None =>
            throw new RuntimeException("The InfoGain has not been fitted to the data. " +
              "This is necessary to compute the Information Gain for each attribute.")
        }
      }
    }
  }

  implicit def transformVectorsInfoGain[T <: Vector : BreezeVectorConverter : TypeInformation : ClassTag] = {
    new TransformDataSetOperation[InfoGainTransformer, T, T] {
      override def transformDataSet(instance: InfoGainTransformer, transformParameters: ParameterMap, input: DataSet[T]): DataSet[T] = input
    }
  }

  /**
    * Computes the Information Gain for each attribute  in the [[DataSet]]
    *
    * @param input    Input data
    * @param selectNF Number of features to select, the `selectNF` will be selected in base of its InfoGain
    * @param nf       Total number of features for the [[DataSet]]
    * @return A tuple where the first element is the total entropy for the [[DataSet]] and a [[Vector]] with
    *         the same length as the number of attributes for this [[DataSet]].
    *         Each value corresponds with the InfoGain for the attribute,
    *         for example the ith value of the [[Vector]] is the InfoGain for attribute  i.
    */
  private[this] def computeInfoGain(
                                     input: DataSet[LabeledVector],
                                     selectNF: Int,
                                     nf: Int): (Double, immutable.Vector[Double]) = {
    require(selectNF < nf, "Features to select must be less than total features")

    // Compute entropy for entire dataset
    val inputFreqs1 = frequencies(input, (x: LabeledVector) => x.label)
    val inputFreqs = inputFreqs1.flatten
    val inputH = entropy(inputFreqs)


    // Compute InfoGain for each attribute
    val gains = (0 until nf).map { i =>
      val freqs = frequencies(input, (x: LabeledVector) => x.vector(i))
      val classH = freqs map entropy
      val hCLassTemp = inputFreqs.zip(classH.reverse).map(x => x._1 / inputFreqs.sum * x._2).sum

      inputH - hCLassTemp
    }.toVector

    inputH -> gains
  }

  /**
    * Computes the frequencies of each attribute value with respect to the label.
    *
    * For example: [[https://stackoverflow.com/a/35105461/1612432]]
    *
    * {{{
    *                        Dataset
    *                         /   \
    *                        /     \
    *                       / Temp° \
    *                      /         \
    *                     /           \
    *                    /             \
    *                  high           low
    *
    *
    *
    * temperature | wind | class                 temperature |wind |class
    * high        | low  | play                      low     | low |play
    * high        | low  | play                      low     | high|cancelled
    * high        | high | cancelled                 low     | low |play
    * high        | low  | play
    * }}}
    *
    * Will compute for the left side frequencies of 3/4 & 1/4
    *
    * @param input [[DataSet]] to extract frequencies of
    * @param f     Function to group by.
    * @return A matrix with the frequencies. Seq(0) its the class frequencies for one split
    *         and so on.
    */
  private[this] def frequencies(input: DataSet[LabeledVector], f: LabeledVector => Double): Seq[Seq[Double]] = {
    import org.apache.flink.api.scala._
    // Group each attribute by its value to compute H(Class | Attr)
    val attrFreqs = input.
      groupBy(f).
      reduceGroup {
        // Count label frequencies for each value of Attr
        (in, out: Collector[(Int, Int)]) =>
          val features = in.toVector
          val groupClass = features groupBy (_.label)
          val x = groupClass mapValues (_.size)
          for (k <- x.keys)
            out.collect(x(k) -> x.values.sum)
      }

    // Extract _._2 from attrFreqs (The frequency for that value)
    attrFreqs.collect()
      .groupBy(_._2)
      .values
      .map(x => x.map(_._1.toDouble))
      .toSeq
  }
}

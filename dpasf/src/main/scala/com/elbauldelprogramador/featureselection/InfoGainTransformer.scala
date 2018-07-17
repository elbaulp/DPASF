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
import org.apache.flink.api.scala.{ DataSet, _ }
import org.apache.flink.ml.common.{ LabeledVector, Parameter, ParameterMap }
import org.apache.flink.ml.math.{ BreezeVectorConverter, DenseVector, Vector }
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.reflect.ClassTag

/**
 * Select the most important features based on its Information Gain.
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
  private[featureselection] var nInstances: Option[Long] = None

  /**
   * Sets the number of features to select
   *
   * @param n Number of features
   * @return [[InfoGainTransformer]]
   */
  def setSelectNF(n: Int): InfoGainTransformer = {
    parameters add (SelectNF, n)
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
    parameters add (NFeatures, n)
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
    val defaultValue: Option[Int] = Some(1)
  }

  // ==================================== Factory methods ==========================================
  def apply(): InfoGainTransformer = new InfoGainTransformer

  // ========================================== Operations =========================================

  /**
   * Trains the [[InfoGainTransformer]] by computing the total entropy of the Data, which is of type
   * [[LabeledVector]] and then calculating the Information Gain for each attribute.
   *
   * The InfoGain is then used in the transformation to select the attributes with most InfoGain.
   */
  implicit val fitLabeledVectorInfoGain = new FitOperation[InfoGainTransformer, LabeledVector] {
    override def fit(instance: InfoGainTransformer, fitParameters: ParameterMap, input: DataSet[LabeledVector]): Unit = {

      val resultingParameters = instance.parameters ++ fitParameters
      val selectNF = resultingParameters(SelectNF)
      val nf = resultingParameters(NFeatures)

      instance.nInstances = Some(input.count)

      val (h, gains) = computeInfoGain(input, instance.nInstances.get, selectNF, nf)

      instance.H = Some(h)
      instance.gains = Some(gains)
    }
  }

  /**
   * [[TransformDataSetOperation]] which select the top N attributes specified by parameters from the DataSet,
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

        require(selectNF <= nf, "Features to select must be less than total features")

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

  /**
   * Computes the Information Gain for each attribute  in the [[DataSet]]
   *
   * @param input      Input data
   * @param nInstances Total number of instances of the [[DataSet]]
   * @param selectNF   Number of features to select, the `selectNF` will be selected in base of its InfoGain
   * @param nf         Total number of features for the [[DataSet]]
   * @return A tuple where the first element is the total entropy for the [[DataSet]] and a [[Vector]] with
   *         the same length as the number of attributes for this [[DataSet]].
   *         Each value corresponds with the InfoGain for the attribute,
   *         for example the ith value of the [[Vector]] is the InfoGain for attribute  i.
   */
  private[this] def computeInfoGain(
    input: DataSet[LabeledVector],
    nInstances: Long,
    selectNF: Int,
    nf: Int): (Double, immutable.Vector[Double]) = {
    require(selectNF <= nf, "Features to select must be less than total features")

    // Compute entropy for entire dataset
    val inputFreqs = frequencies(input, (x: LabeledVector) => x.label)
      .map(x => x._1 -> x._2.head).sortBy(_._1)
    val inputH = entropy(inputFreqs map (_._2))
    // Compute InfoGain for each attribute
    val gains = (0 until nf).map { i =>
      val freqs = frequencies(input, (x: LabeledVector) => x.vector(i)).sortBy(_._1)
      val px = freqs.map(x => x._1 -> x._2.sum / nInstances).sortBy(_._1)
      val attrH = freqs.map(x => x._1 -> entropy(x._2)).sortBy(_._1)
      // Compute H(Class | Attr)
      val HYAttr = px.map(_._2).zip(attrH.map(_._2))
        .foldLeft(0.0)((h, x) => h + (x._1 * x._2))
      inputH - HYAttr
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
   * --------------------------                 ------------------------
   * high        | low  | play                  low         | low |play
   * high        | low  | play                  low         | high|cancelled
   * high        | high | cancelled             low         | low |play
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
  private[this] def frequencies(
    input: DataSet[LabeledVector],
    f: LabeledVector => Double): Seq[(Double, Seq[Double])] = {
    import org.apache.flink.api.scala._
    // Group each attribute by its value to compute H(Class | Attr)
    val attrFreqs = input.
      groupBy(f).
      reduceGroup {
        // Count label frequencies for each value of Attr
        (in, out: Collector[(Double, Seq[Double])]) =>
          val features = in.toVector
          val groupClass = features groupBy (_.label)
          val x = groupClass.mapValues(_.size.toDouble).toSeq.map(features(0).vector(0) -> _._2)
            .groupBy(_._1)
          for (k <- x.keys)
            out.collect(k -> x(k).map(_._2))
      }
    attrFreqs.collect
  }
}

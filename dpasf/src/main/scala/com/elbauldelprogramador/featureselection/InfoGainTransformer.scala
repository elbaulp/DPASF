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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{ DataSet, _ }
import org.apache.flink.ml.common.{ LabeledVector, Parameter, ParameterMap }
import org.apache.flink.ml.math.{ BreezeVectorConverter, DenseVector, Vector }
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }
import org.apache.flink.api.java.aggregation.Aggregations.SUM
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.reflect.ClassTag

class InfoGainTransformer extends Transformer[InfoGainTransformer] {

  import InfoGainTransformer._

  private[featureselection] var selectedFeatures: Option[immutable.Vector[Int]] = None

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
    val defaultValue: Option[Int] = None
  }

  // ==================================== Factory methods ==========================================
  def apply(): InfoGainTransformer = new InfoGainTransformer

  // ========================================== Operations =========================================

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   * @param n     Number of elements
   *
   */
  private[featureselection] def entropy(freqs: Seq[Long], n: Long) = {
    freqs.aggregate(0.0)({
      case (h, q) =>
        h + (if (q == 0) 0 else (q.toDouble / n) * (math.log(q.toDouble / n) / math.log(2)))
    }, { case (h1, h2) => h1 + h2 }) * -1
  }

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   */
  private[featureselection] def entropy(freqs: Seq[Long]): Double =
    entropy(freqs, freqs.sum)

  implicit def fitLabeledVectorInfoGain = new FitOperation[InfoGainTransformer, LabeledVector] {
    override def fit(instance: InfoGainTransformer, fitParameters: ParameterMap, input: DataSet[LabeledVector]): Unit = {
      //      val initMap = mutable.Map.empty[Key, Double]
      //      val r = input.map {
      //        v =>
      //          v.vector.foldLeft(initMap) {
      //            case (m, (i, value)) =>
      //              val key = Key(value, v.label)
      //              val cval = m.getOrElseUpdate(key, .0) + 1.0
      //              m += (key -> cval)
      //          }
      //      }
      //      instance.counts = Some(r)
      val resultingParameters = instance.parameters ++ fitParameters
      val selectNF = resultingParameters(SelectNF)
      val nf = resultingParameters(NFeatures)
      val counts = input groupBy (_.label) reduceGroup (_.length.toLong)
      val H = entropy(counts.collect)
      log.error(s"Entrpy: $H")

      instance.selectedFeatures = Some(immutable.Vector(1))
    }
  }

  private[this] def frequencies(input: DataSet[LabeledVector]): immutable.Vector[Double] = {

    ???
  }

  /**
   * Computes the Information Gain for each attribute  in the [[DataSet]]
   *
   * @param input    Input data
   * @param selectNF Number of features to select, the `selectNF` will be selected in base of its InfoGain
   * @param nf       Total number of features for the [[DataSet]]
   * @return A [[Vector]] with the same length as the number of attributes for this [[DataSet]].
   *         Each value corresponds with the InfoGain for the attribute,
   *         for example the ith value of the [[Vector]] is the InfoGain for attribute  i.
   */
  private[this] def computeInfoGain(
    input: DataSet[LabeledVector],
    selectNF: Int,
    nf: Int): immutable.Vector[Double] = {
    //require(selectNF < nf, "Features to select must be less than total features")
    ???
  }

  implicit def fitVectorInfoGain[T <: Vector] = new FitOperation[InfoGainTransformer, T] {
    override def fit(instance: InfoGainTransformer, fitParameters: ParameterMap, input: DataSet[T]): Unit =
      input
  }

  implicit def transformDataSetLabeledVectorsInfoGain = {
    new TransformDataSetOperation[InfoGainTransformer, LabeledVector, LabeledVector] {
      override def transformDataSet(
        instance: InfoGainTransformer,
        transformParameters: ParameterMap,
        input: DataSet[LabeledVector]): DataSet[LabeledVector] = {

        val resultingParameters = instance.parameters ++ transformParameters

        instance.selectedFeatures match {
          case Some(features) =>
            input.map { x =>
              val attrs = x.vector.map(_._2).toVector
              val newf = features.map(attrs(_))
              LabeledVector(x.label, DenseVector(newf.toArray))
            }
          case None =>
            throw new RuntimeException("The InfoGain has not been fitted to the data.")
        }

        //        instance.counts match {
        //          case Some(counts) =>
        //            println(s"INSIDE transform!!!")
        //            val lastcounts = counts.collect().last
        //            println(s"This is last: $lastcounts")
        //            input.mapWithBcVariable(counts) {
        //              (x, _) =>
        //                val javaMap = lastcounts.mapValues(Double.box)
        //                val infoGain = IncrementalInfoGain.applySelection(javaMap)
        //                println(infoGain)
        //                x
        //            }
        //          case None =>
        //            throw new RuntimeException("The InfoGain has not been fitted to the data.")
        //        }
      }
    }
  }

  implicit def transformVectorsInfoGain[T <: Vector: BreezeVectorConverter: TypeInformation: ClassTag] = {
    new TransformDataSetOperation[InfoGainTransformer, T, T] {
      override def transformDataSet(instance: InfoGainTransformer, transformParameters: ParameterMap, input: DataSet[T]): DataSet[T] = input
    }
  }
}

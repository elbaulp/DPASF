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

import java.{ lang, util }

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{ DataSet, _ }
import org.apache.flink.ml._
import org.apache.flink.ml.common.{ LabeledVector, Parameter, ParameterMap }
import org.apache.flink.ml.math.{ BreezeVectorConverter, DenseVector, Vector }
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }

import scala.collection.JavaConversions._
import collection.JavaConverters._
import scala.collection.convert.Decorators
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.reflect.ClassTag

class InfoGainTransformer extends Transformer[InfoGainTransformer] {

  import InfoGainTransformer._

  private[featureselection] var counts: Option[DataSet[mutable.Map[Key, Double]]] = None

  /**
   * Sets the number of neighbors for KNN
   *
   * @param  k the number of neighbors to train KNN with.
   * @return itself
   */
  def setK(k: Int): InfoGainTransformer = {
    parameters add (K, k)
    this
  }

  /**
   * Sets if attributes should be binarized instead of discretized
   *
   * @param b Whether it should be binarized or not
   * @return itself
   */
  def setBinarize(b: Boolean): InfoGainTransformer = {
    parameters add (Binarize, b)
    this
  }
}

/**
 * Companion object of InfoGain. Contains convenience functions and the parameter
 * type definitions of the algorithm
 */
object InfoGainTransformer {

  // ====================================== Parameters =============================================
  case object K extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(3)
  }

  case object Binarize extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = Some(false)
  }

  // ==================================== Factory methods ==========================================
  def apply(): InfoGainTransformer = new InfoGainTransformer

  // ========================================== Operations =========================================

  implicit def fitLabeledVectorInfoGain = new FitOperation[InfoGainTransformer, LabeledVector] {
    override def fit(instance: InfoGainTransformer, fitParameters: ParameterMap, input: DataSet[LabeledVector]): Unit = {
      val initMap = mutable.Map.empty[Key, Double]
      val r = input.map {
        v =>
          v.vector.foldLeft(initMap) {
            case (m, (i, value)) =>
              println("INSIDE fit!!!")
              val key = Key(value, v.label)
              val cval = m.getOrElseUpdate(key, .0) + 1.0
              m += (key -> cval)
          }
      }
      instance.counts = Some(r)
    }
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
        val Kvalue = resultingParameters(K)
        val binarize = resultingParameters(Binarize)

        instance.counts match {
          case Some(counts) =>
            println(s"INSIDE transform!!!")
            val lastcounts = counts.collect().last
            println(s"This is last: $lastcounts")
            input.mapWithBcVariable(counts) {
              (x, _) =>
                val javaMap = lastcounts.mapValues(Double.box)
                val infoGain = IncrementalInfoGain.applySelection(javaMap)
                println(infoGain)
                x
            }
          case None =>
            throw new RuntimeException("The InfoGain has not been fitted to the data.")
        }
      }
    }
  }

  implicit def transformVectorsInfoGain[T <: Vector: BreezeVectorConverter: TypeInformation: ClassTag] = {
    new TransformDataSetOperation[InfoGainTransformer, T, T] {
      override def transformDataSet(instance: InfoGainTransformer, transformParameters: ParameterMap, input: DataSet[T]): DataSet[T] = input
    }
  }

  case class Key(x: Double, y: Double) {
    //    override def equals(o: scala.Any): Boolean = {
    //      if (this == o) true
    //      if (!o.isInstanceOf[Key]) false
    //      val key = o.asInstanceOf[Key]
    //      (x == key.x) && (y == key.y)
    //    }

    override def hashCode(): Int =
      31 * java.lang.Float.floatToIntBits(x.toFloat) + java.lang.Float.floatToIntBits(y.toFloat)
  }

}
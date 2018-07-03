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
import com.elbauldelprogramador.utils.SamplingUtils
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.slf4j.LoggerFactory

/**
 * Incremental Discretization Algorithm
 *
 * @param nBins number of bins
 * @param s sample size
 *
 */
case class IDADiscretizer(
  nAttrs: Int,
  nBins: Int = 5,
  s: Int = 5) extends Serializable {

  private[this] val log = LoggerFactory.getLogger(this.getClass)
  private[this] val V = Vector.tabulate(nAttrs)(i => new IntervalHeapWrapper(nBins, i))
  private[this] val randomReservoir = SamplingUtils.reservoirSample((1 to s).toList.iterator, 1)

  /**
   * Discretize the given LabeledVector
   *
   * @param v LabeledVector to discretize
   *
   * @return LabeledVector in which each value corresponds
   * with the bin the value was discretized to.
   */
  private[this] def updateSamples(v: LabeledVector): LabeledVector = {
    val attrs = v.vector.map(_._2)
    val label = v.label
    // TODO: Check for missing values
    attrs
      .zipWithIndex
      .foldLeft(LabeledVector(label, DenseVector.init(attrs size, -1000))) {
        case (lv, (x, i)) =>
          //          if (V(i).getNbSamples < s) {
          V(i) insertValue x // insert
          lv.vector.update(i, V(i) getBin x)
          lv
        //          } else {
        //            if (randomReservoir(0) <= s / (i + 1)) {
        //val randVal = Random nextInt s
        //V(i) replace (randVal, attr)
        //              V(i) insertValue d
        //              lv
        //            }
        //            lv
        //          }
      }
  }

  /**
   * Computes the current cutpoints for the discretization
   *
   * @param x LabeledVector to wich compute its cutpoints
   *
   * @return A Vector[IntervalHeapWrapper] containing the discretized data
   */
  private[this] def computeCutPoints(x: LabeledVector): Vector[IntervalHeapWrapper] = {
    val attrs = x.vector.map(_._2)
    val label = x.label
    attrs
      .zipWithIndex
      .foldLeft(V) {
        case (iv, (v, i)) =>
          iv(i) insertValue v
          iv
      }
  }

  /**
   * Map a value to its corresponding bin
   *
   * @param vs LabeledVector attributes
   * @param cuts The cutpoints for each attribute and its bins
   *
   * @return The attributes assigned to its bins
   */
  private[this] def assignDiscreteValue(vs: Seq[Double], cuts: Seq[Seq[Double]]): Seq[Double] =
    vs.zipWithIndex.map {
      case (v, i) =>
        (cuts(i) indexWhere (v <= _)).toDouble
    }

  def discretizeWith(cuts: Vector[Vector[Double]], data: DataSet[LabeledVector]): DataSet[LabeledVector] =
    data map { l =>
      val attrs = l.vector.map(_._2).toSeq
      val discretized = assignDiscreteValue(attrs, cuts)
      LabeledVector(l.label, DenseVector(discretized.toArray))
    }

  /**
   * Return the cutpoints for the discretization
   *
   * @param data The Dataset to obtain the cutpoints from.
   *
   * @return A Vector[Vector[Double]] containing the cutpoints for each bin
   */
  def cutPoints(data: DataSet[LabeledVector]): Vector[Vector[Double]] =
    data.map(computeCutPoints _)
      .collect
      .last.map(_.getBoundaries.toVector)

  def discretize(data: DataSet[LabeledVector]): DataSet[LabeledVector] =
    data.map(updateSamples _)

}

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

import scala.util.Random

import com.elbauldelprogramador.datastructures.IntervalHeapWrapper
import com.elbauldelprogramador.utils.SamplingUtils
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.slf4j.LoggerFactory

/**
 * Incremental Discretization Algorithm
 *
 * @param data DataSet to sample from
 * @param nBins number of bins
 * @param s sample size
 *
 */
case class IDADiscretizer(
  nAttrs: Int,
  nBins: Int = 5,
  s: Int = 5) extends Serializable {

  private[this] val log = LoggerFactory.getLogger(this.getClass)
  //private[this] val V = Vector.tabulate(nAttrs)(i => IntervalHeapWrapper(nBins, i, s))
  private[this] val V = Vector.tabulate(nAttrs)(i => new IntervalHeapWrapper(nBins, s, i))
  val randomReservoir = SamplingUtils.reservoirSample((1 to s).toList.iterator, 1)

  def updateSamples(v: LabeledVector) /*: Vector[IntervalHeap]*/ = {
    val attrs = v.vector.map(_._2)
    val label = v.label
    // TODO: Check for missing values
    attrs
      .zipWithIndex
      .foreach {
        case (attr, i) =>
          if (V(i).getNbSamples < s) {
            V(i) insertValue (attr) // insert
          } else {
            if (randomReservoir(0) <= s / (i + 1)) {
              val randVal = Random nextInt (s)
              // TODO V(i) replace (randVal, attr)
              V(i) insertValue (attr)
            }
          }
      }
    V
  }

  def discretize(data: DataSet[LabeledVector]) /*: DataSet[IntervalHeap]*/ = {
    data map (x => updateSamples(x))
  }
}

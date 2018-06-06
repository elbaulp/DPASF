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

import com.elbauldelprogramador.utils.SamplingUtils
//import com.google.common.collect.MinMaxPriorityQueue
import org.apache.flink.shaded.guava18.com.google.common.collect.MinMaxPriorityQueue
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
 * Incremental Discretization Algorithm
 *
 * @param data DataSet to sample from
 * @param nBins number of bins
 * @param s sample size
 *
 */
case class IDADiscretizer[T](
  data: DataSet[T],
  nBins: Int = 5,
  s: Int = 1000) extends Serializable {

  private[this] val log = LoggerFactory.getLogger("IDADiscretizer")
  private[this] val V = Vector.tabulate(10)(_ => IntervalHeap(nBins, 1, 1, s))

  def test = {
    //V(0).insert(1.0)
    //V(0).insert(300.0)
    //V(0).insert(301.0)
    //V(0).insert(4.0)
    //V(0).insert(100.0)
    //V(0).insert(105.0)
    //V(0).insert(6.0)
    //V(0).insert(7.0)
    //V(0).insert(100.0)
    //V(0).insert(8.0)
    //V(0).insert(9.0)
    //V(0) insert (10)
    //V foreach (println)
    val r = SamplingUtils.reservoirSample(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).iterator, 1)

  }

  private[this] def updateSamples(x: T): Vector[IntervalHeap] = {
    log.warn(s"$x")
    //SamplingUtils.reservoirSample(input: Iterator[T], k: Int, seed: Long)
    log.warn(s"${x.getClass.getFields.length}")
    V
  }

  def discretize() = {
    //data flatMap (x => updateSamples(x))
    data map (x => updateSamples(x))

  }
}

/**
 * A MinMaxPriorityQueue storing the Discretization. Indexd by attribute
 *
 * Each MinMaxPriorityQueue V, $V_i^j$ stores the values for the j-th bin
 * of $X_i$
 *
 * @param bins Number of bins to use for discretize
 * @param attrIndex Index of the attribute
 * @param n Number of instancess seen so far
 * @param s sample size
 */
private[discretizers] case class IntervalHeap(
  private val nBins: Int,
  private val attrIndex: Int,
  private val nbSamples: Int, // nbSamples
  private val sampleSize: Int) extends Serializable { // sampleSize

  type jDouble = java.lang.Double

  private[this] val log = LoggerFactory.getLogger(this.getClass)

  private lazy val V: Vector[MinMaxPriorityQueue[jDouble]] =
    Vector.tabulate(nBins)(_ => MinMaxPriorityQueue.create())

  def insert(value: Double): Unit = {
    // Count how many elements are in V
    val Vsize = V.map(_.size).sum
    // Find the target bin
    val t = Vsize % nBins

    //[1.0;6.0](2) [2.0;7.0](2) [3.0;8.0](2) [4.0;9.0](2) [5.0;10.0](2)
    //[1.0;1.0](1) [2.0;2.0](1) [3.0;3.0](1) [4.0;4.0](1) [5.0;10.0](6)

    val v = V filter (q => !q.isEmpty)
    val j =
      // V is empty completely, insert first value in first qeue
      if (v.isEmpty) 0
      else {
        // Advance while new value > min value in each bin
        val s = v.filter(_.peekFirst < value)
        // If at the end, do not overflow
        if (s.size < nBins - 1) s.size
        else nBins - 1
      }

    //var j_ = j
    //while (j_ < t && value >= V(j_).peekLast) j_ = j_ + 1
    //log.debug(s"$j_")

    val vv = V slice (j, t)
    val nj =
      if (!vv.isEmpty) V indexOf (vv minBy (_.peekLast < value))
      else j
    // [1.0;2.0](2) [3.0;4.0](2) [5.0;6.0](2) [7.0;8.0](2) [9.0;10.0](2)
    //var j_ = nj
    //var t_ = t
    //if (t_ >= j_) {
    //  while (j_ < t_) {
    //    val d = V(t_ - 1).pollLast
    //    V(t_) add d
    //    t_ = t_ - 1
    //  }
    //} else {
    //  while (j_ > t_) {
    //    val d = V(t_ + 1).pollFirst
    //    V(t_) add d
    //    t_ = t_ + 1
    //  }
    //}
    // Shuffle excess values up
    //[1.0;4.0](4) [5.0;5.0](1) [;][9.0;9.0](1) [6.0;10.0](4)
    //[1.0;2.0](2) [3.0;4.0](2) [5.0;6.0](2) [7.0;8.0](2) [9.0;10.0](2)
    if (t >= j) {
      log.debug(s"VALUES UP: t = $t, j = $j, slice = ${V slice (j, t)}, $V")
      V.zipWithIndex.
        slice(j, t).
        foreach {
          case (q, i) =>
            log.debug(s"\t\t($q, $i)")
            V(i + 1) add (q.pollLast)
        }
    } // Shuffle excess values down
    else {
      log.debug(s"VALUES DOWN: t = $t, j = $j, slice = ${V slice (t, j)}, $V")
      V.zipWithIndex.
        slice(t, j).
        foreach {
          case (q, i) =>
            V(i) add (V(i + 1) pollFirst)
            log.debug(s"\t\t($q, $i)")
        }
    }

    V(j) add value
    // Check order /w Delta Iterate Operator flink
  }

  override def toString: String = {
    val buff = new StringBuffer()
    buff.append("Attr [" + attrIndex + "] \t")
    V.foreach {
      case v =>
        if (!v.isEmpty()) {
          buff.append("[" +
            v.peekFirst +
            ";" +
            v.peekLast +
            "](" + v.size + ") ")
        } else {
          buff.append("[;]")
        }
    }
    buff.toString
  }
}

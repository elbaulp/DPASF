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

import scala.annotation.{ switch, tailrec }
import scala.util.Random

import com.elbauldelprogramador.utils.SamplingUtils
import com.google.common.collect.MinMaxPriorityQueue
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
  s: Int = 10) {

  private[this] val log = LoggerFactory.getLogger("IDADiscretizer")
  private[this] val V = Vector.tabulate(nAttrs)(i => IntervalHeap(nBins, i, s))
  private[this] val randomReservoir = SamplingUtils.reservoirSample((1 to s).toList.iterator, 1)

  private[this] def updateSamples(v: LabeledVector) /*: Vector[IntervalHeap]*/ = {
    val attrs = v.vector.map(_._2)
    // TODO: Check for missing values
    attrs
      .zipWithIndex
      .foreach {
        case (attr, i) =>
          if (V(i).nInstances < s) {
            V(i) insert (attr)
          } else {
            if (randomReservoir(0) <= s / (i + 1)) {
              val randVal = Random nextInt (s)
              V(i) replace (randVal, attr)
            }
          }
      }
  }

  def discretize(data: DataSet[LabeledVector]) /*: DataSet[IntervalHeap]*/ = {
    val d = data map (x => updateSamples(x))
    d print
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
private[this] case class IntervalHeap(
  private val nBins: Int,
  private val attrIndex: Int,
  //  private val nbSamples: Int, // nbSamples
  private val sampleSize: Int) extends Serializable { // sampleSize

  type jDouble = java.lang.Double

  private[this] val log = LoggerFactory.getLogger(this.getClass)
  private[this] lazy val V: Vector[MinMaxPriorityQueue[jDouble]] =
    Vector.tabulate(nBins)(_ => MinMaxPriorityQueue.create())
  private[this] var nSamples = 0

  private[this] def shuffleUp(iBin: Int, tBin: Int): Unit = {
    @tailrec
    def go(bin: Int): Unit =
      if (iBin < bin) {
        V(bin) add (V(bin - 1) pollLast)
        go(bin - 1)
      }
    go(tBin)
  }

  private[this] def shuffleDown(iBin: Int, tBin: Int): Unit = {
    @tailrec
    def go(bin: Int): Unit =
      if (iBin > bin) {
        V(bin) add (V(bin + 1) pollFirst)
        go(bin + 1)
      }
    go(tBin)
  }

  /**
   * Insert the value into the IntervalHeap
   *
   * @param value the value to add
   */
  def insert(value: Double): Unit = {
    log.debug(s"insert($value): $V")
    val targetbin = nSamples % nBins

    // TODO Improve this code
    var loc = 0; /// < the bin into which this value goes

    // advance while v can't go into this bin
    while (loc < nBins - 1 && !V(loc + 1).isEmpty && value > V(loc + 1).peekFirst) {
      loc += 1
    }

    // no bin before targetbin can be empty
    while (loc < targetbin && value >= V(loc).peekLast) {
      // v falls between intervals so insert into the one closer to the
      // target
      loc += 1
    }
    // END Improve this code

    val insertBin = loc

    if (targetbin >= insertBin) {
      // Shuffle values up
      shuffleUp(insertBin, targetbin)
    } else {
      // Shuffle values down
      shuffleDown(insertBin, targetbin)
    }

    V(insertBin) add value
    nSamples += 1

    checkIntervalHeap
  }

  /**
   * Replace the ith value by v
   *
   * @param i the index of the value to replace
   * @param v the replacement value
   */
  def replace(i: Int, v: Double) = {
    log.info(s"replace($i, $v), $V")
    // Find the target bin
    val (bin, index) = findTBin(i)
    // Find the value
    val value = V(bin).toArray.apply(index) match {
      case x: jDouble => x
      case _ =>
        log.error("A double was expected ")
        throw new ClassCastException("A double was expected ")

    }
    log.debug(s"Removing $value from bin #$bin")
    V(bin).remove(value)
    // Find bin for new value
    val intervals = V filter (q => !q.isEmpty)
    val newBin =
      // V is empty completely, insert first value in first qeue
      if (intervals.isEmpty) 0
      else {
        // Advance while new value > min value in each bin
        val s = intervals.filter(_.peekFirst < value)
        // If at the end, do not overflow
        if (s.size < nBins - 1) s.size
        else nBins - 1
      }
    log.debug(s"newbin =  $newBin, oldBin = $bin")

    if (bin >= newBin) {
      // Shuffle excess values up
      V.zipWithIndex.
        slice(newBin, bin).
        foreach {
          case (q, i) =>
            V(i + 1) add (q.pollLast)
        }
    } // Shuffle excess values down
    else {
      V.zipWithIndex.
        slice(bin, newBin).
        foreach {
          case (_, i) =>
            V(i) add (V(i + 1) pollFirst)
        }
    }
    log.debug(s"Adding $v to bin #$newBin")
    V(newBin) add v
    nSamples += 1

    checkIntervalHeap
  }

  def nInstances: Int = nSamples //V.map(_.size).sum

  /**
   * Find in which bin is the element located at index i
   *
   * @param i ith element
   * @return A tuple with the target bin and the relative index within that bin
   */
  private[this] def findTBin(i: Int): Tuple2[Int, Int] = {
    @tailrec
    def go(v: Vector[MinMaxPriorityQueue[jDouble]], z: Int, tBin: Int): Tuple2[Int, Int] = {
      v match {
        case h +: t if z >= h.size =>
          go(t, z - h.size, tBin + 1)
        case _ =>
          (tBin, z)
      }
    }
    go(V, i, 0)
  }

  private[this] def checkIntervalHeap = {
    log.debug(s"${this.toString}")
    V.zipWithIndex
      .slice(0, V.size - 1)
      .foreach {
        case (q, i) =>
          if (!q.isEmpty && !V(i + 1).isEmpty) {
            // Check Size
            if (q.size < V(i + 1).size)
              log.warn(s"Wrong size: ${q.size} < ${V(i + 1).size}")
            if (q.peekLast > V(i + 1).peekFirst)
              log.warn(s"Wrong order: ${q.peekLast} > ${V(i + 1).peekFirst}")
          }
      }
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

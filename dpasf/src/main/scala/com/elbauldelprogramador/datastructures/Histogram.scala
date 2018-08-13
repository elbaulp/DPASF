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

package com.elbauldelprogramador.datastructures

import org.apache.flink.ml.math.DenseMatrix

// TODO: DOC
// TODO: TEST
case class Histogram(
  nRows: Int,
  nCols: Int,
  min: Int,
  step: Double,
  private val countMatrix: DenseMatrix,
  private val cutMatrix: DenseMatrix,
  private val classDistribMatrix: Array[Array[Map[Int, Double]]])
  extends Serializable {

  import Histogram._

  def updateCounts(row: Int, col: Int, value: Double): Unit =
    countMatrix update (row, col, value)
  def updateCuts(row: Int, col: Int, value: Double): Unit =
    cutMatrix update (row, col, value)
  def updateClassDistrib(row: Int, col: Int, label: Int): Unit = {
    val currClassDistrib = classDistribMatrix(row)(col).getOrElse(label, 0.0) + 1
    val newClassDistrib = classDistribMatrix(row)(col) + (label -> currClassDistrib)
    classDistribMatrix(row).update(col, newClassDistrib)
  }

  def counts(row: Int, col: Int): Double = countMatrix apply (row, col)
  def cuts(row: Int, col: Int): Double = cutMatrix apply (row, col)
  def classDistrib(row: Int, col: Int): Double = ???

  def ++(h: Histogram): Histogram = {
    val newCuts = DenseMatrix(nRows, nCols, (h.cutMatrix.data, cutMatrix.data).zipped.map(_ + _))
    val newCounts = DenseMatrix(nRows, nCols, (h.countMatrix.data, countMatrix.data).zipped.map(_ + _))
    val newClassDistrib = (h.classDistribMatrix, classDistribMatrix).zipped.map { (a, b) =>
      (a, b).zipped.map { (x, y) =>
        val merged = x.toSeq ++ y.toSeq
        merged.groupBy(_._1).mapValues(_.map(_._2).sum)
      }
    }
    apply(min, step, newCounts, newCuts, newClassDistrib)
  }

  override def toString: String = {

    val classDistrBuff = StringBuilder.newBuilder

    for (row <- classDistribMatrix.indices) {
      classDistrBuff.append(s"Attr $row: \n\t")
      val maps = classDistribMatrix(row).filter(_.nonEmpty).mkString
      classDistrBuff.append(maps)
      classDistrBuff.append("\n")
    }

    s"Histogram: => " +
      "Counts [\n" + countMatrix.toString + "]" +
      "Cuts [ \n" + cutMatrix.toString + "]" +
      "classDistrib [ \n" + classDistrBuff + " ]"
  }
}

object Histogram {
  def apply(nRows: Int, nCols: Int, min: Int, step: Double): Histogram =
    Histogram(
      nRows,
      nCols,
      min,
      step,
      DenseMatrix.zeros(nRows, nCols),
      DenseMatrix(nRows, nCols,
        Array.tabulate(nRows, nCols)((_, j) => min + j.toDouble % nCols * step).transpose.flatten),
      Array.fill(nRows, nCols)(Map.empty[Int, Double]))

  def apply(
    min: Int,
    step: Double,
    counts: DenseMatrix,
    cutpoints: DenseMatrix,
    classDistrib: Array[Array[Map[Int, Double]]]): Histogram =
    Histogram(
      counts.numRows,
      counts.numCols,
      min,
      step,
      counts,
      cutpoints,
      classDistrib)
}

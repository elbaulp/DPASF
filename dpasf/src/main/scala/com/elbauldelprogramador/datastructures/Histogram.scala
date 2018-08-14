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
import scala.collection.mutable.ArrayBuffer

// TODO: DOC
// TODO: TEST
case class Histogram(
  nRows: Int,
  nCols: Int,
  min: Int,
  step: Double,
  private val countMatrix: Array[ArrayBuffer[Double]],
  private val cutMatrix: Array[ArrayBuffer[Double]],
  private val classDistribMatrix: Array[ArrayBuffer[Map[Int, Double]]])
  extends Serializable {

  def updateCounts(row: Int, col: Int, value: Double): Unit =
    countMatrix(row).update(col, value)
  def updateCuts(row: Int, col: Int, value: Double): Unit =
    cutMatrix(row).update(col, value)
  def updateClassDistrib(row: Int, col: Int, newDist: Map[Int, Double]): Unit =
    classDistribMatrix(row).update(col, newDist)
  def updateClassDistrib(row: Int, col: Int, label: Int): Unit = {
    val currClassDistrib = classDistribMatrix(row)(col).getOrElse(label, 0.0) + 1
    val newClassDistrib = classDistribMatrix(row)(col) + (label -> currClassDistrib)
    classDistribMatrix(row).update(col, newClassDistrib)
  }

  def addCounts(row: Int, col: Int, value: Double): Unit =
    countMatrix(row).insert(col, value)
  def addCuts(row: Int, col: Int, value: Double): Unit =
    cutMatrix(row).insert(col, value)
  def addClassDistrib(row: Int, col: Int, newDist: Map[Int, Double]): Unit =
    classDistribMatrix(row).insert(col, newDist)

  def prependCut(i: Int, value: Double): Unit =
    cutMatrix(i).prepend(value)
  def prependCounts(i: Int, value: Double): Unit =
    countMatrix(i).prepend(value)
  def prependClassDist(i: Int, newDist: Map[Int, Double]): Unit =
    classDistribMatrix(i).prepend(newDist)
  def appendCut(i: Int, value: Double): Unit =
    cutMatrix(i).append(value)
  def appendCounts(i: Int, value: Double): Unit =
    countMatrix(i).append(value)
  def appendClassDist(i: Int, newDist: Map[Int, Double]): Unit =
    classDistribMatrix(i).append(newDist)

  def counts(row: Int, col: Int): Double = countMatrix(row)(col)
  def cuts(row: Int, col: Int): Double = cutMatrix(row)(col)
  def classDistrib(row: Int, col: Int): Map[Int, Double] = classDistribMatrix(row)(col)

  //  def ++(h: Histogram): Histogram = {
  //    val newCuts = DenseMatrix(nRows, nCols, (h.cutMatrix.data, cutMatrix.data).zipped.map(_ + _))
  //    val newCounts = DenseMatrix(nRows, nCols, (h.countMatrix.data, countMatrix.data).zipped.map(_ + _))
  //    val newClassDistrib = (h.classDistribMatrix, classDistribMatrix).zipped.map { (a, b) =>
  //      (a, b).zipped.map { (x, y) =>
  //        val merged = x.toSeq ++ y.toSeq
  //        merged.groupBy(_._1).mapValues(_.map(_._2).sum)
  //      }
  //    }
  //    apply(min, step, newCounts, newCuts, newClassDistrib)
  //  }

  def nColumns(i: Int) = cutMatrix(i).size

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
      Array.fill(nRows)(ArrayBuffer.fill(nCols + 1)(0)),
      Array.fill(nRows)(ArrayBuffer.tabulate(nCols + 1)(min + _.toDouble * step)),
      Array.fill(nRows)(ArrayBuffer.fill(nCols + 1)(Map.empty[Int, Double])))
}

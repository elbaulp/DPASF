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

import scala.collection.mutable.ArrayBuffer

// TODO: DOC
// TODO: TEST
case class Histogram(
  nRows: Int,
  nCols: Int,
  min: Int,
  step: Double,
  private val countMatrix: Array[ArrayBuffer[Double]],
  private val cutMatrixL1: Array[ArrayBuffer[Double]],
  val distribMatrixL1: Array[ArrayBuffer[Map[Int, Double]]],
  private val distribMatrixL2: Array[ArrayBuffer[Map[Int, Double]]],
  private val cutMatrixL2: ArrayBuffer[ArrayBuffer[Double]])
  extends Serializable {

  def updateCounts(row: Int, col: Int, value: Double): Unit =
    countMatrix(row).update(col, value)
  def updateCuts(row: Int, col: Int, value: Double): Unit =
    cutMatrixL1(row).update(col, value)
  def updateCutsL2(row: Int, col: Int, value: Double): Unit =
    cutMatrixL2(row).update(col, value)
  def updateCutsL2(row: Int, newCol: ArrayBuffer[Double]): Unit =
    cutMatrixL2.update(row, newCol)
  def updateClassDistribL1(row: Int, col: Int, newDist: Map[Int, Double]): Unit =
    distribMatrixL1(row).update(col, newDist)
  def updateClassDistribL1(row: Int, col: Int, label: Int): Unit = {
    val currClassDistrib = distribMatrixL1(row)(col).getOrElse(label, 0.0) + 1
    val newClassDistrib = distribMatrixL1(row)(col) + (label -> currClassDistrib)
    distribMatrixL1(row).update(col, newClassDistrib)
  }
  def updateClassDistribL2(row: Int, col: Int, newDist: Map[Int, Double]): Unit =
    distribMatrixL2(row).update(col, newDist)
  def updateClassDistribL2(row: Int, col: Int, label: Int): Unit = {
    val currClassDistrib = distribMatrixL2(row)(col).getOrElse(label, 0.0) + 1
    val newClassDistrib = distribMatrixL2(row)(col) + (label -> currClassDistrib)
    distribMatrixL2(row).update(col, newClassDistrib)
  }

  def addCounts(row: Int, col: Int, value: Double): Unit =
    countMatrix(row).insert(col, value)
  def addCuts(row: Int, col: Int, value: Double): Unit =
    cutMatrixL1(row).insert(col, value)
  def addCutsL2(row: Int, col: Int, value: Double): Unit =
    cutMatrixL2(row).insert(col, value)
  def addClassDistribL1(row: Int, col: Int, newDist: Map[Int, Double]): Unit =
    distribMatrixL1(row).insert(col, newDist)
  def addClassDistribL2(row: Int, col: Int, newDist: Map[Int, Double]): Unit =
    distribMatrixL2(row).insert(col, newDist)

  def clearCutsL2: Unit = cutMatrixL2.clear
  def clearCutsL2(i: Int): Unit = cutMatrixL2(i).clear

  def prependCut(i: Int, value: Double): Unit =
    cutMatrixL1(i).prepend(value)
  def prependCutL2(i: Int, value: Double): Unit =
    cutMatrixL2(i).prepend(value)
  def prependCounts(i: Int, value: Double): Unit =
    countMatrix(i).prepend(value)
  def prependClassDistribL1(i: Int, newDist: Map[Int, Double]): Unit =
    distribMatrixL1(i).prepend(newDist)
  def prependClassDistribL2(i: Int, newDist: Map[Int, Double]): Unit =
    distribMatrixL2(i).prepend(newDist)

  def appendCut(i: Int, value: Double): Unit =
    cutMatrixL1(i).append(value)
  def appendCutL2(i: Int, value: Double): Unit =
    cutMatrixL2(i).append(value)
  def appendCounts(i: Int, value: Double): Unit =
    countMatrix(i).append(value)
  def appendClassDistL1(i: Int, newDist: Map[Int, Double]): Unit =
    distribMatrixL1(i).append(newDist)
  def appendClassDistL2(i: Int, newDist: Map[Int, Double]): Unit =
    distribMatrixL2(i).append(newDist)

  def counts(row: Int, col: Int): Double = countMatrix(row)(col)
  def cuts(row: Int, col: Int): Double = cutMatrixL1(row)(col)
  def cutsL2(row: Int, col: Int): Double = cutMatrixL2(row)(col)
  def cutsL2(row: Int): ArrayBuffer[Double] = cutMatrixL2(row)
  def classDistribL1(row: Int, col: Int): Map[Int, Double] = distribMatrixL1(row)(col)
  def classDistribL2(row: Int, col: Int): Map[Int, Double] = distribMatrixL2(row)(col)

  // TODO make all bellow private and compute entropy publicly
  def greatestClass(attrIdx: Int, first: Int, l: Int): Int = {
    def before(attrIdx: Int, first: Int, l: Int) = {
      val slice = distribMatrixL1(attrIdx).slice(first, l)
      if (slice.nonEmpty) {
        val r = slice.maxBy { x ⇒
          if (x.nonEmpty)
            x.keysIterator.max
          else
            0
        }.keySet
        if (r.isEmpty) 1 else r.max + 1
      } else 1
    }

    var numClasses = 0
    var i = first
    while ({
      i < l
    }) {
      val classDist = distribMatrixL1(attrIdx)(i)
      import scala.collection.JavaConversions._
      for (key ← classDist.keySet) {
        if (key > numClasses) numClasses = key
      }

      {
        i += 1; i - 1
      }
    }
    assert(before(attrIdx, first, l) == numClasses + 1)
    numClasses + 1
  }

  def classCounts(attrIdx: Int, first: Int, l: Int): Map[Int, Double] = {
    def before = {
      // https://stackoverflow.com/a/1263028
      val counts = distribMatrixL1(attrIdx).slice(first, l)

      val r = mergeMap(counts)((x, y) ⇒ x + y)
      if (r.isEmpty) Map(-1 -> 0d)
      else r
    }

    //    import java.util
    val counts = Array.fill(2)(Array.fill(greatestClass(attrIdx, first, l))(0d))
    var i = first
    while ({
      i < l
    }) {
      val classDist = distribMatrixL1(attrIdx)(i)
      import scala.collection.JavaConversions._
      for (entry ← classDist.entrySet) {
        counts(1)(entry.getKey) += entry.getValue
        //        numInstances += entry.getValue
      }
      {
        i += 1; i - 1
      }
    }
    assert(counts(1).sorted sameElements before.values.toSeq.sorted)
    before
  }

  // https://stackoverflow.com/a/1264772
  private[this] def mergeMap[A, B](ms: Seq[Map[A, B]])(f: (B, B) ⇒ B): Map[A, B] =
    (Map.empty[A, B] /: (for (m ← ms; kv ← m) yield kv)) { (a, kv) ⇒
      a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
    }

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

  def nColumns(i: Int): Int = cutMatrixL1(i).size

  //  override def toString: String = {
  //
  //    val classDistrBuff = StringBuilder.newBuilder
  //
  //    for (row <- distribMatrixL1.indices) {
  //      classDistrBuff.append(s"Attr $row: \n\t")
  //      val maps = distribMatrixL1(row).filter(_.nonEmpty).mkString
  //      classDistrBuff.append(maps)
  //      classDistrBuff.append("\n")
  //    }
  //
  //    s"Histogram: => " +
  //      "Counts [\n" + countMatrix.toString + "]" +
  //      "Cuts [ \n" + cutMatrixL1.toString + "]" +
  //      "classDistrib [ \n" + classDistrBuff + " ]"
  //  }
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
      Array.fill(nRows)(ArrayBuffer.fill(nCols + 1)(Map.empty[Int, Double])),
      Array.fill(nRows)(ArrayBuffer.fill(nCols + 1)(Map.empty[Int, Double])),
      ArrayBuffer.fill(nRows)(ArrayBuffer.tabulate(nCols + 1)(min + _.toDouble * step)))
}

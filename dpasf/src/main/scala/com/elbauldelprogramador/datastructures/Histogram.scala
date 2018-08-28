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
  countMatrix: Array[ArrayBuffer[Double]],
  cutMatrixL1: Array[ArrayBuffer[Double]],
  distribMatrixL1: Array[ArrayBuffer[Map[Int, Double]]],
  distribMatrixL2: Array[ArrayBuffer[Map[Int, Double]]],
  cutMatrixL2: ArrayBuffer[ArrayBuffer[Double]])
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
  def updateClassDistribL2(row: Int, newDistrib: ArrayBuffer[Map[Int, Double]]): Unit =
    distribMatrixL2.update(row, newDistrib)

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

  def clearCutsL2(newSize: Int): Unit = {
    cutMatrixL2.clear
    for (_ ← 0 until newSize) {
      cutMatrixL2.append(ArrayBuffer.empty)
    }
  }
  def addNewCutsL2(i: Int, newSize: Int): Unit =
    cutMatrixL2(i).append(ArrayBuffer.fill(newSize)(0d): _*)

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
  def cuts(row: Int): ArrayBuffer[Double] = cutMatrixL1(row)
  def cutsL2(row: Int, col: Int): Double = cutMatrixL2(row)(col)
  def cutsL2(row: Int): ArrayBuffer[Double] = cutMatrixL2(row)
  def classDistribL1(row: Int, col: Int): Map[Int, Double] = distribMatrixL1(row)(col)
  def classDistribL2(row: Int, col: Int): Map[Int, Double] = distribMatrixL2(row)(col)

  // TODO make all bellow private and compute entropy publicly
  private[this] def greatestClass(attrIdx: Int, first: Int, l: Int): Int = {
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
    assert(before(attrIdx, first, l) == numClasses + 1, "before(attrIdx, first, l) == numClasses + 1")
    numClasses + 1
  }

  def classCounts(attrIdx: Int, first: Int, l: Int): Vector[Vector[Double]] = {
    def before = {
      val nClasses = greatestClass(attrIdx, first, l)
      // https://stackoverflow.com/a/1263028
      val counts = distribMatrixL1(attrIdx).slice(first, l)

      val r = mergeMap(counts)((x, y) ⇒ x + y)

      val row0 = Vector.fill(nClasses)(0d)
      val row1 = Vector.tabulate(nClasses)(k ⇒ r.getOrElse(k, 0d))

      Vector(row0, row1)
      //      if (!r.contains(0) || r.isEmpty) r + (0 -> 0d)
      //      else r
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
    assert(counts(1) sameElements before(1), "counts(1).sorted sameElements before.values.toSeq.sorted")
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
  def cutsSize: Int = cutMatrixL1.size

  override def toString: String = {

    val classDistrBuff = StringBuilder.newBuilder
    val cutsBuff = StringBuilder.newBuilder
    //
    for (row ← distribMatrixL1.indices) {
      classDistrBuff.append(s"Attr $row: \n\t")
      val maps = distribMatrixL1(row).filter(_.nonEmpty).mkString
      classDistrBuff.append(maps)
      classDistrBuff.append("\n")
    }
    cutMatrixL2.zipWithIndex.foreach {
      case (b, i) ⇒
        cutsBuff.append(s"[Attr $i => ")
        cutsBuff.append(b.size + "]\n")
    }
    //
    //        for (i <- cutMatrixL2.indices) {
    //          cutsBuff.append("[ ")
    //          cut
    //        }
    //
    //        val x = for {
    //          i <- cutMatrixL2
    //          c <- i
    //          cutsBuff.append("[ ")
    //          cutsBuff.append(c)
    //          cutsBuff.append("[")
    //        } yield "[ " + c + "["
    //        x

    s"Histogram: => " +
      //          "Counts [\n" + countMatrix.toString + "]" +
      //          "Cuts [ \n" + cutMatrixL1.toString + "]" +
      //          "classDistrib [ \n" + classDistrBuff + " ]" +
      "Final Cuts [ \n" + cutsBuff
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
      Array.fill(nRows)(ArrayBuffer.fill(nCols + 1)(Map.empty[Int, Double])),
      Array.fill(nRows)(ArrayBuffer.fill(nCols + 1)(Map.empty[Int, Double])),
      ArrayBuffer.fill(nRows)(ArrayBuffer.tabulate(nCols + 1)(min + _.toDouble * step)))
}

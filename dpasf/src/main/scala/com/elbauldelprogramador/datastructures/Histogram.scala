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
case class Histogram(nRows: Int,nCols: Int, nClasses: Int) extends Serializable {

  val counts = DenseMatrix.zeros(nRows, nCols)
  val cutpoints = DenseMatrix.zeros(nRows, nCols)
  val matrixClass = DenseMatrix.zeros(nClasses, nCols)

  def updateCounts(row: Int, col: Int, value: Double): Unit =
    counts update(row, col, value)
  def updateCuts(row: Int, col: Int, value: Double): Unit =
    cutpoints update(row, col, value)
  def updateClassMatrix(row: Int, col: Int, value: Double): Unit =
    matrixClass update(row, col, value)

}

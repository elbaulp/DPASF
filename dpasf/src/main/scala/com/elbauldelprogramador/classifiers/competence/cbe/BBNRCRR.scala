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

package com.elbauldelprogramador.classifiers.competence.cbe

import jcolibri.method.maintenance.algorithms.{ BBNRNoiseReduction, CRRRedundancyRemoval }
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.Parameter
import org.apache.flink.ml.pipeline.Predictor
import org.slf4j.LoggerFactory

/**
 * A case-base maintained by a maintenance method composed of two methods:
 * BBNR and CRR.
 */
class BBNRCRR extends Predictor[BBNRCRR] {

  import BBNRCRR._

  private[this] val log = LoggerFactory.getLogger(this.getClass)
  private[this] val bbnr = new BBNRNoiseReduction
  private[this] val crr = new CRRRedundancyRemoval

  /**
   * Sets the number of neighbors for KNN
   *
   * @param  k the number of neighbors to train KNN with.
   * @return itself
   */
  def setNeighbors(k: Int): BBNRCRR = {
    parameters add (Neighbors, k)
    this
  }

  /**
   * Sets the size of the environment.
   *
   * @param p The size of the environment
   * @return  itself
   */
  def setPeriod(p: Int): BBNRCRR = {
    parameters add (Period, p)
    this
  }

  def runTwoStepCaseBasedEditMethod = ???
  def initCaseBase = ???
  def createCase = ???
  def initData = ???
  def train = ???
  def getInstanceVotes = ???
}

/**
 * Companion object of BBNRCRR. Contains convenience functions and the parameter
 * type definitions of the algorithm
 */
object BBNRCRR {

  // ==================== Parameters ================
  case object Neighbors extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(3)
  }

  case object Period extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(500)
  }

  // =================== Factory Methods ============

  def apply(): BBNRCRR = new BBNRCRR
}

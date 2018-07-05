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
import org.slf4j.LoggerFactory

/**
 * A case-base maintained by a maintenance method composed of two methods:
 * BBNR and CRR.
 *
 * @constructor BBNRCRR
 *
 * @param k Number of neighbors used in search
 * @param p Size of the environments
 *
 */
case class BBNRCRR(k: Int, p: Int) {

  private[this] val log = LoggerFactory.getLogger(this.getClass)
  private[this] val bbnr = new BBNRNoiseReduction
  private[this] val crr = new CRRRedundancyRemoval

  def runTwoStepCaseBasedEditMethod = ???
  def initCaseBase = ???
  def createCase = ???
  def initData = ???
  def train = ???
  def getInstanceVotes = ???
}

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

import com.google.common.collect.MinMaxPriorityQueue
import org.apache.flink.api.scala._
import org.apache.flink.util.XORShiftRandom
import scala.reflect.ClassTag
import scala.util.Random
import com.elbauldelprogramador.utils.SamplingUtils._

case class IDADiscretizer[T: ClassTag](
  data: DataSet[T],
  s: Int = 1000,
  private val V: Vector[MinMaxPriorityQueue[Long]]) extends Serializable {


  private[this] def updateSamples(x: T) = reservoirSample(Iterator(1), 1)
  private[this] def insertValue(x: Long) = ???
}

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

package com.elbauldelprogramador.utils

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector

private[elbauldelprogramador] case object FlinkUtils {

  /**
   * Return the number of attributes in a [[DataSet]] in a efficient way
   *
   * Thanks to https://stackoverflow.com/a/51497661/1612432
   *
   * @param dataset The [[DataSet]] to compute the number of Attributes from.
   * @return The number of attributes
   */
  def numAttrs(dataset: DataSet[LabeledVector]): Int = dataset
    // only forward first vector of each partition
    .mapPartition(in ⇒ if (in.hasNext) Seq(in.next) else Seq())
    .name("Foward first vector/part")
    // move all remaining vectors to a single partition, compute size of the first and forward it
    .mapPartition(in ⇒ if (in.hasNext) Seq(in.next.vector.size) else Seq())
    .name("Compute NumAttrs")
    .setParallelism(1)
    .collect
    .head

}

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

package com.elbauldelprogramador

import java.util.concurrent.TimeUnit
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.{ DataSet, ExecutionEnvironment, _ }
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

object Setup {
  val env = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(12)

  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    2, // number of restart attempts
    Time.of(5, TimeUnit.SECONDS) // delay
  ))

  //  val path =
  val datasets = "skin_nonsking" :: "covtype" :: "ht_sensor" :: "watch_acc" :: "watch_gyr" :: Nil
  // covtype from 1
  // skin_nonsking from 1

  //  skin_nonsking-5-fold/skin_nonsking-5-1tra.data
  def readFold(k: Int, ds: String, path: String = "file:///home/aalcalde/datasets/"): (DataSet[LabeledVector], DataSet[LabeledVector]) = {
    val rawDataTrain = env.readTextFile(path + s"$ds-5-fold/$ds-5-${k}tra.data/part-00000").name("Raw Train")
    val rawDataTest = env.readTextFile(path + s"$ds-5-fold/$ds-5-${k}tst.data/part-00000").name("Raw Test")

    val train = rawDataTrain.map { line ⇒
      val array = line split ","
      val arrayDouble = array map (_.toDouble)
      if (ds == datasets.head || ds == datasets(1)) {
        LabeledVector(arrayDouble.last - 1, DenseVector(arrayDouble.init))
      } else {
        LabeledVector(arrayDouble.last, DenseVector(arrayDouble.init))
      }
    }.name("Map train")

    val test = rawDataTest.map { line ⇒
      val array = line split ","
      val arrayDouble = array map (_.toDouble)
      if (ds == datasets.head || ds == datasets(1)) {
        LabeledVector(arrayDouble.last - 1, DenseVector(arrayDouble.init))
      } else {
        LabeledVector(arrayDouble.last, DenseVector(arrayDouble.init))
      }
    }.name("Map Test")

    (train, test)
  }

}

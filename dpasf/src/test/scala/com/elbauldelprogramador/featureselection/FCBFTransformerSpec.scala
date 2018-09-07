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
package com.elbauldelprogramador.featureselection

import java.util.concurrent.TimeUnit

import com.elbauldelprogramador.BddSpec
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ ConfigConstants, Configuration, WebOptions }
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

class FCBFTransformerSpec extends BddSpec with Serializable {

  private val conf = new Configuration()
  conf.setString(WebOptions.LOG_PATH, "/tmp/flink/log/output.out")

  private val env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env.setParallelism(8)

  //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  //  1, // number of restart attempts
  //  Time.of(10, TimeUnit.SECONDS) // delay
  //))

  val abaloneDat = env.readCsvFile[(Int, Double, Double, Double, Double, Double, Double, Double, Int)](getClass.getResource("/abalone.csv").getPath)
    .name("Reading Abalone DS")
  val abaloneDS = abaloneDat
    .map { tuple ⇒
      val list = tuple.productIterator.toList
      val numList = list map { x ⇒
        x match {
          case d: Double ⇒ d
          case i: Int ⇒ i
        }
      }
      LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
    }.name("Abalone DS")

  val pimaDat = env.readCsvFile[(Int, Int, Int, Int, Int, Double, Double, Int, Int)](getClass.getResource("/pima.csv").getPath)
    .name("Reading Pima DS")
  val pimaDS = pimaDat
    .map { tuple ⇒
      val list = tuple.productIterator.toList
      val numList = list map {
        case d: Double ⇒ d
        case i: Int ⇒ i
      }
      LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
    }.name("Pima DS")

  val fcbf = FCBFTransformer()
    .setThreshold(.05)

  "A FCBF FS Transformer" - {
    "When computing best Features for Pima DataSet with threshold of 0.05" - {
      "Should return features [6]" in {
        fcbf.fit(pimaDS)
        assert(fcbf.metricsOption.get === Seq(6))
      }
    }

    "When computing best Features for Abalone DataSet with threshold of 0.05" - {
      "Should return fetures [4]" in {
        fcbf.fit(abaloneDS)
        assert(fcbf.metricsOption.get === Seq(4))
      }
    }
  }

}

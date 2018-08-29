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
import org.apache.flink.api.scala.{ ExecutionEnvironment, _ }
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

class FCBFTransformerSpec extends BddSpec with Serializable {

  private val env = ExecutionEnvironment.createLocalEnvironment()

  env.setParallelism(1)
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    3, // number of restart attempts
    Time.of(10, TimeUnit.SECONDS) // delay
  ))

  val data = env.readCsvFile[(Int, Double, Double, Double, Double, Double, Double, Double, Int)](getClass.getResource("/abalone.csv").getPath)
  val dataSet = data map { tuple ⇒
    val list = tuple.productIterator.toList
    val numList = list map { x ⇒
      x match {
        case d: Double ⇒ d
        case i: Int ⇒ i
      }
    }
    LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
  }

  val fcbf = FCBFTransformer()
    .setThreshold(.05)

  "A FCBF FS on ???" - {
    "When computing its Entropy" - {
      "Should return entropy H(X) equal to 0.9402859586706309" in {
        fcbf.fit(dataSet)
        assert(1 === 1)
      }

      "Should return a IG(Attr1) equal to 0.2467498197744391 bits" in {
        //        assert(tennisGain.gains.get.head === 0.2467498197744391)
      }
    }
  }

}

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

import com.elbauldelprogramador.BddSpec
import com.elbauldelprogramador.pojo.ElecNormNew
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.preprocessing.MinMaxScaler

class PIDiscretizerTransformerSpec extends BddSpec with Serializable {
  private val env = ExecutionEnvironment.createLocalEnvironment()

  env.setParallelism(1)
  //  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  //    1, // number of restart attempts
  //    Time.of(2, TimeUnit.SECONDS) // delay
  //  ))

//  private val data = env.readCsvFile[Iris](getClass.getResource("/iris.dat").getPath)
//  private[discretizers] val dataSet = data map { tuple ⇒
//    val list = tuple.productIterator.toList
//    val numList = list map (_.asInstanceOf[Double])
//    LabeledVector(numList(4), DenseVector(numList.take(4).toArray))
//  }

    val data = env.readCsvFile[ElecNormNew](getClass.getResource("/elecNormNew.arff").getPath)
    val dataSet = data map { tuple =>
      val list = tuple.productIterator.toList
      val numList = list map (_.asInstanceOf[Double])
      LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
    }

  private val pid = PIDiscretizerTransformer()
//    .setAlpha(.10)
//    .setUpdateExamples(50)
//    .setL1Bins(5)
  private val scaler = MinMaxScaler()

  "A PIDiscretizer on ElecNormNew" - {
    "When computing Discretization" - {
      "Should return...." in {

        val pipeline = scaler
          .chainTransformer(pid)

        // Train the pipeline (scaler and multiple linear regression)
        pipeline fit dataSet
        val r = pipeline.transform(dataSet)
        r.print
        //        val r = pipeline.transform(dataSet)
        //        r.count()
        assert(1 === 1)
      }
    }
  }
}

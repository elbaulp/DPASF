package com.elbauldelprogramador.featureselection

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

import java.util.concurrent.TimeUnit

import com.elbauldelprogramador.BddSpec
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

class InfoGainSpec extends BddSpec with Serializable {

  private val env = ExecutionEnvironment.createLocalEnvironment()

  env.setParallelism(1)
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    3, // number of restart attempts
    Time.of(10, TimeUnit.SECONDS) // delay
  ))

  //  private val data = Vector(
  //    Vector("high", "low", "play"),
  //    Vector("low", "low", "play"),
  //    Vector("high", "low", "play"),
  //    Vector("low", "high", "cancelled"),
  //    Vector("low", "low", "play"),
  //    Vector("high", "high", "cancelled"),
  //    Vector("high", "low", "play")
  //  )

  private val data = Vector(
    Vector("1", "0", "10"),
    Vector("0", "0", "10"),
    Vector("1", "0", "10"),
    Vector("0", "1", "20"),
    Vector("0", "0", "10"),
    Vector("1", "1", "20"),
    Vector("1", "0", "10"))

  val dataSet = env.fromCollection(data map { tuple =>
    val list = tuple.iterator.toList
    val numList = list map (_.toDouble)
    LabeledVector(numList(2), DenseVector(numList.take(2).toArray))
  })

  val gain = InfoGainTransformer()
    .setNFeatures(2)
    .setSelectNF(1)
  gain.fit(dataSet)

  "A Information Gain FS on DataSet1" - {
    "When computing its Entropy" - {
      "Should return entropy H(X) equal to 0.863120568566631" in {
        assert(gain.H.get === 0.863120568566631)
      }

      "Should return a IG(Attr1) equal to 0.02126595565168199 bits" in {
        assert(gain.gains.get(0) === 0.02126595565168199)
      }

      "Should return a IG(Attr2) equal to 0.863120568566631 bits" in {
        assert(gain.gains.get(1) === 0.863120568566631)
      }

      "Should return Attr2 as the most important feature" in {
        val result = gain.transform(dataSet)
        val vector = result.collect
          .toVector
          .flatMap(_.vector.toVector.map(_._2))

        assert( data.map(_(1).toDouble) == vector)
      }
    }
  }
}


// Thanks to http://blog.ssanj.net/posts/2016-07-06-how-to-run-scalacheck-from-scalatest-and-generate-html-reports.html
// for help me use scalacheck from scalatest
//class CategoryPropSpec extends CheckSpec {
//  import fixtures._
//
//  property("a == Id(a)") {
//    check(forAll { i:String =>
//      Category.Id(i) === i
//    })
//  }
//
//  property("Id∘f = f") {
//    check(forAll { i: Int =>
//      Category.Id(square(i)) === square(i)
//    })
//  }
//
//  property("f∘Id = f") {
//    check(forAll { i: Int =>
//      f(Category.Id(i)) === f(i)
//    })
//  }
//
//  property("Associativity: h∘(g∘f) = (h∘g)∘f = h∘g∘f"){
//    check(forAll { i: Int =>
//      Category.compose(Category.compose(f, g), h)(i) === Category.compose(f, Category.compose(g, h))(i)
//    })
//  }
//}

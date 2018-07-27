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

import com.elbauldelprogramador.BddSpec
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.ml.MLUtils
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

class OFSGDTransformerSpec extends BddSpec with Serializable {

  private val env = ExecutionEnvironment.createLocalEnvironment()

  env.setParallelism(1)
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    3, // number of restart attempts
    Time.of(10, TimeUnit.SECONDS) // delay
  ))

  private val data = MLUtils.readLibSVM(env, getClass.getResource("/svmguide3").getPath)

  private val ofs = OFSGDTransformer()
    .setNFeature(5)

  "A Information Gain FS on TennnisDS" - {
    "When computing its Entropy" - {
      "Should return entropy H(X) equal to 0.9402859586706309" in {
        val r = ofs transform data
        r.print
        assert(r === r)
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

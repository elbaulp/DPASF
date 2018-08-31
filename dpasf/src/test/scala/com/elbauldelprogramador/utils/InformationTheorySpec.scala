package com.elbauldelprogramador.utils

import com.elbauldelprogramador.BddSpec
import com.elbauldelprogramador.utils.InformationTheory._
import java.util.concurrent.TimeUnit
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

class InformationTheorySpec extends BddSpec with Serializable {

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

  val column0 = dataSet.map(lv ⇒ LabeledVector(lv.label, DenseVector(lv.vector(0))))

  "Informaion Theroy Spec" - {
    "When computing entropy for the first column of Abalone" - {
      "Should return entropy H(X) equal to 0.9474026952708241" in {
        assert(entropy(column0) === 0.9474026952708241)
      }
    }
    "When computing conditional entropy on the first column with label H(X|Y)" - {
      "Should be 0.9215406320671156" in {
        assert(conditionalEntropy(column0) === 0.9215406320671156)
      }
    }
  }

}

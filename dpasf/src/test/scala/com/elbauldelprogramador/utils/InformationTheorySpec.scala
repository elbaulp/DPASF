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

  env.setParallelism(4)
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

  // https://stackoverflow.com/a/11107546/1612432
  private def truncateAt(n: Double, p: Int): Double = {
    val s = math pow (10, p)
    (math floor n * s) / s
  }

  val column0 = dataSet.map(lv ⇒ (lv.label, lv.vector(0)))

  "Informaion Theroy Spec" - {
    "When computing entropy for the first column of Abalone" - {
      "Should return entropy H(X) equal to 0.9474026952708241" in {
        assert(entropy(column0.map(_._2)) === 0.9474026952708241)
      }
    }

    "When computing conditional entropy on the first column with label H(X|Y)" - {
      "Should be 0.9215406320671156" in {
        assert(conditionalEntropy(column0) === 0.9215406320671156)
      }
    }

    "When computing Mutual Information on the first column with label" - {
      "Should be 0.02586206320370854" in {
        assert(mutualInformation(column0) === 0.02586206320370854)
      }
    }

    "When computing Symmetrical Uncertainty on the first column with label" - {
      "Should be 0.026560697288523276" in {
        assert(symmetricalUncertainty(column0) === 0.026560697288523276)
      }
    }

    "When computing Symmetrical Uncertainty on the whole Abalone Dataset" - {
      """Should be [
              0.026560697288523276,
              0.06473920415289718,
              0.07117682463410091,
              0.09063844964208025,
              0.11157317642876234,
              0.08092568050702133,
              0.07495399595384908,
              0.09506775221909011
         ]""" in {
        val expected = List(
          0.026560697288523276,
          0.06473920415289718,
          0.07117682463410091,
          0.09063844964208025,
          0.11157317642876234,
          0.08092568050702133,
          0.07495399595384908,
          0.09506775221909011).map(truncateAt(_, 6))

        val su = for (i ← 0 until 8) yield {
          val attr = dataSet.map(lv ⇒ (lv.label, lv.vector(i)))
          InformationTheory.symmetricalUncertainty(attr)
        }

        assert(su.map(truncateAt(_, 6)) === expected)
      }
    }
  }

}

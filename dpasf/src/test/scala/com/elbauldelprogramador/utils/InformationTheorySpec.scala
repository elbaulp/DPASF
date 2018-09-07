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

  env.setParallelism(8)
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
  def roundAt(p: Int)(n: Double): Double = {
    val s = math pow (10, p)
    (math round n * s) / s
  }
  def truncateAt(p: Int)(n: Double): Double = {
    val s = math pow (10, p)
    (math floor n * s) / s
  }

  def truncAt2(n: Double) = truncateAt(2)(n)
  def roundAt4(n: Double) = roundAt(4)(n)
  def roundAt2(n: Double) = roundAt(2)(n)

  val column0 = dataSet.map(lv ⇒ (lv.label, lv.vector(0)))
  val column0v = column0.collect
  val y = column0v map (_._1)
  val x = column0v map (_._2)

  "Informaion Theroy Spec" - {
    "When computing entropy for the first column of Abalone" - {
      "Should return entropy H(X) equal to 0.9474" in {
        assert(roundAt4(entropy(x)) === 0.9474)
      }
    }

    "When computing conditional entropy on the first column with label H(X|Y)" - {
      "Should be 0.9215" in {
        assert(roundAt4(conditionalEntropy(x, y)) === 0.9215)
      }
    }

    "When computing Mutual Information on the first column with label" - {
      "Should be 0.0259" in {
        assert(roundAt4(mutualInformation(x, y)) === 0.0259)
      }
    }

    "When computing Symmetrical Uncertainty on the first column with label" - {
      "Should be 0.0266" in {
        assert(roundAt4(symmetricalUncertainty(column0)) === 0.0266)
      }
    }

    "When computing Symmetrical Uncertainty on the whole Abalone Dataset" - {
      """Should be [
              0.02,
              0.06,
              0.07,
              0.09,
              0.11,
              0.08,
              0.07,
              0.09
         ]""" in {
        val expected = List(
          0.02,
          0.06,
          0.07,
          0.09,
          0.11,
          0.08,
          0.07,
          0.09).map(truncAt2)

        val su = for (i ← 0 until 8) yield {
          val attr = dataSet.map(lv ⇒ (lv.label, lv.vector(i)))
          InformationTheory.symmetricalUncertainty(attr)
        }

        assert(su.map(truncAt2) === expected)
      }
    }
  }

}

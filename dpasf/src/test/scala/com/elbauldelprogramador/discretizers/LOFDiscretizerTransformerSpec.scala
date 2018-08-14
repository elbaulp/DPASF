package com.elbauldelprogramador.discretizers

import java.util.concurrent.TimeUnit

import com.elbauldelprogramador.BddSpec
import com.elbauldelprogramador.pojo.ElecNormNew
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

class LOFDiscretizerTransformerSpec extends BddSpec with Serializable {
  private val env = ExecutionEnvironment.createLocalEnvironment()

  env.setParallelism(1)
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    3, // number of restart attempts
    Time.of(10, TimeUnit.SECONDS) // delay
  ))

  val data = env.readCsvFile[ElecNormNew](getClass.getResource("/elecNormNew.arff").getPath)
  val dataSet = data map { tuple =>
    val list = tuple.productIterator.toList
    val numList = list map (_.asInstanceOf[Double])
    LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
  }

  private val ofs = LOFDiscretizerTransformer()
    .setInitTh(1)

  "A Information Gain FS on TennnisDS" - {
    "When computing its Entropy" - {
      "Should return entropy H(X) equal to 0.9402859586706309" in {

        val discretized = ofs transform (dataSet)
        discretized.print
      }
    }
  }
}

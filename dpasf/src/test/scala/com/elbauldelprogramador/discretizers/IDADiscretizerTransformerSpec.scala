package com.elbauldelprogramador.discretizers

import java.util.concurrent.TimeUnit

import com.elbauldelprogramador.BddSpec
import com.elbauldelprogramador.pojo.{ ElecNormNew, Iris }
import com.elbauldelprogramador.Setup._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

object fixtures extends Serializable {
  //val env = ExecutionEnvironment.getExecutionEnvironment
  private val env = ExecutionEnvironment.createLocalEnvironment()
  env.setParallelism(4)
  //  env.getConfig.enableObjectReuse
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    1, // number of restart attempts
    Time.of(10, TimeUnit.SECONDS) // delay
  ))

    private val data = env.readCsvFile[Iris](getClass.getResource("/iris.dat").getPath)
    private[discretizers] val dataSet = data map { tuple ⇒
      val list = tuple.productIterator.toList
      val numList = list map (_.asInstanceOf[Double])
      LabeledVector(numList(4), DenseVector(numList.take(4).toArray))
    }

//  val data = env.readCsvFile[ElecNormNew](getClass.getResource("/elecNormNew.arff").getPath)
//  val dataSet = data map { tuple ⇒
//    val list = tuple.productIterator.toList
//    val numList = list map (_.asInstanceOf[Double])
//    LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
//  }

}

// BDD tests
class IDADiscretizerTransformerSpec extends BddSpec with Serializable {

  import fixtures._

  "A IDA Discretization" - {
    "When computing its discretization" - {
      "Should be computed correctly" in {
        val ida = IDADiscretizerTransformer()
          .setBins(5).setSampleSize(15)
        ida fit dataSet
        val discretized = ida transform dataSet
        val discretized2 = ida transform dataSet

        assert(!discretized.collect.exists(_.vector.exists(_._2 == -1)))
      }
    }
  }
}

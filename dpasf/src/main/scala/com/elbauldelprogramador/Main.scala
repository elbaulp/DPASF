package com.elbauldelprogramador

import com.elbauldelprogramador.featureselection.FCBFTransformer
import org.apache.flink.api.scala.{ ExecutionEnvironment, _ }
import org.apache.flink.configuration.{ ConfigOption, Configuration }
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.math.Vector

object Main {

  private[this] case class Iris(
    SepalLength: Double,
    SepalWidth: Double,
    PetalLength: Double,
    PetalWidth: Double,
    Class: Double)

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    //v.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    //3, // number of restart attempts
    //Time.of(10, TimeUnit.SECONDS) // delay
    //

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

    val pimaDat = env.readCsvFile[(Int, Int, Int, Int, Int, Double, Double, Int, Int)](args(0))
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
    fcbf.fit(pimaDS)
    val r = fcbf.transform(pimaDS)
    r.print
    println("DONE")
  }
}

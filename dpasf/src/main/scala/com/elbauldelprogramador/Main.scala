package com.elbauldelprogramador

import com.elbauldelprogramador.Setup._
import com.elbauldelprogramador.discretizers.{ IDADiscretizerTransformer, LOFDiscretizerTransformer, PIDiscretizerTransformer }
import com.elbauldelprogramador.featureselection.{ FCBFTransformer, InfoGainTransformer, OFSGDTransformer }
import com.elbauldelprogramador.utils.FlinkUtils
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.pipeline.Transformer
import org.apache.flink.ml.preprocessing.MinMaxScaler
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

object Main {

  private[this] val log = LoggerFactory.getLogger(Main.getClass.getSimpleName)

  def main(args: Array[String]) {

    args.headOption match {
      case Some(f) ⇒ f.toInt match {
        case x: Int ⇒ doTrain(x)
        case _ ⇒ println(s"First param must be an integer")
      }
      case _ ⇒ println(s"Invalid param")
    }
  }

  def doTrain(k: Int) = {
    val scaler = MinMaxScaler()

    val fcbf = FCBFTransformer()
    val ig = InfoGainTransformer()
    val ofs = OFSGDTransformer() // TODO: Only binary class {-1, 1}
    val ida = IDADiscretizerTransformer().setBins(5)
    val lofd = LOFDiscretizerTransformer() // TODO: SET n class
    val pid = PIDiscretizerTransformer()

    // Preprocess and save them
    //for (d ← datasets) {
    val data = datasets(0)
    val (train, test) = readFold(k, data)
    val nattr = FlinkUtils.numAttrs(train)
    val selectN = (nattr / 2.0).ceil.toInt

    println(s"Will keep 50% of features:, from $nattr to $selectN")
    log.error(s"Will keep 50% of features:, from $nattr to $selectN")

    val trans = pid
    val transName = trans.getClass.getSimpleName

    val pipeline = scaler.
      chainTransformer(trans)

    println(s"Fitting for ${data} with ${transName} for fold $k")
    pipeline fit train

    println("Transforming...")
    val result = pipeline transform train
    println("Done transforming train")

    val testt = pipeline transform test

    println("Done transorming test")

    write(result, s"train-${data}-${transName}-fold-$k")
    println(s"Done train-${data}-${transName}-fold-$k")
    write(testt, s"test-${data}-${transName}-fold-$k")
    println(s"Done test-${data}-${transName}-fold-$k")
    //}
    env.execute
  }

  def write(d: DataSet[LabeledVector], name: String): Unit = {
    d.map { tuple ⇒
      tuple.vector.map(_._2).mkString(",") + "," + tuple.label
    }.name("Spark Format")
      .writeAsText(s"file:///home/aalcalde/prep/$name", WriteMode.OVERWRITE)
      .name(s"Write $name")
      .setParallelism(1)
  }
}

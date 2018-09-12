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

object Main {

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

    val fcbf = FCBFTransformer().setThreshold(.05)
    val ig = InfoGainTransformer()
    val ofs = OFSGDTransformer()
    val ida = IDADiscretizerTransformer().setBins(5)
    val lofd = LOFDiscretizerTransformer()
    val pid = PIDiscretizerTransformer()

    // Preprocess and save them
    //for (d ← datasets) {
    val data = datasets(0)
    val (train, test) = readFold(k, data)
    val nattr = FlinkUtils.numAttrs(train)
    val selectN = (nattr / 2.0).ceil.toInt

    println(s"Will keep 50% of features:, from $nattr to $selectN")

    val trans = fcbf
    val transName = trans.getClass.getSimpleName

    val pipeline = scaler.
      chainTransformer(trans)

    println(s"Fitting for ${data} with ${transName} for fold $k")
    pipeline fit train

    println("Transforming...")
    val result = pipeline transform train
    println("Donde transformint train")

    val testt = pipeline transform test
    println("Donde transformint test")

    write(testt, s"test-${transName}-fold-$k")
    println(s"Done test-${transName}-fold-$k")
    write(result, s"train-${transName}-fold-$k")
    println(s"Done train-${transName}-fold-$k")
    //}
    env.execute
  }

  def write(d: DataSet[LabeledVector], name: String): Unit = {
    d.map { tuple ⇒
      tuple.vector.toArray.mkString(",").replace("(", "").replace(")", "") + "," + tuple.label
    }.writeAsText(s"file:///home/aalcalde/pp/$name", WriteMode.OVERWRITE)
      .name(("Writing"))
      .setParallelism(1)
  }
}

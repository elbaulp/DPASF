package com.elbauldelprogramador

import java.util.concurrent.TimeUnit

import com.elbauldelprogramador.featureselection.InfoGainTransformer
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.{ ExecutionEnvironment, _ }
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.preprocessing.{ PolynomialFeatures, StandardScaler }
import org.apache.flink.ml.regression.MultipleLinearRegression

import scala.util.Random

object Main {

  private[this] case class Iris(
    SepalLength: Double,
    SepalWidth: Double,
    PetalLength: Double,
    PetalWidth: Double,
    Class: Double)

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.createLocalEnvironment(1)
    env.setParallelism(1)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, // number of restart attempts
      Time.of(10, TimeUnit.SECONDS) // delay
    ))

    //    val data = (1 to 10).map(_ => Seq(1, 2, Random.nextInt(2)))
    //    val dataSet: DataSet[LabeledVector] = env.fromCollection(data map { tuple =>
    //      val list = tuple.iterator.toList
    //      val numList = list map (_.asInstanceOf[Double])
    //      LabeledVector(numList(2), DenseVector(numList.take(2).toArray))
    //    })

    val data = env.readCsvFile[Iris](getClass.getResource("/iris.dat").getPath)
    //val dataSet = new ArffFileStream(getClass.getResource("/elecNormNew.arff").getPath, -1)
    //  val data = (1 to 10).map(_ => Seq(Random.nextDouble, Random.nextDouble, Random.nextInt))
    //  val dataSet = env.fromCollection(data map { tuple =>
    //    val list = tuple.iterator.toList
    //    val numList = list map (_.asInstanceOf[Double])
    //    LabeledVector(numList(2), DenseVector(numList.take(2).toArray))
    //  })
    //val data1 = env.fromCollection(data)
    val dataSet = data map { tuple =>
      val list = tuple.productIterator.toList
      val numList = list map (_.asInstanceOf[Double])
      LabeledVector(numList(4), DenseVector(numList.take(4).toArray))
    }

    //
    //    val ida = IDADiscretizer(nAttrs = 2)
    //    val r = ida.discretize(dataSet)
    //    //val rr = r map (x => x.map(_.peekLast))
    //
    //    //    r print
    //

    //val data = Source.fromString(getClass.getResource("textual/spam/corpus600.zip").getPath)
    //    val r = new CRRFull(new BBNRNoiseReduction, new CRRRedundancyRemoval)
    //    val config = new KNNClassificationConfig()
    //    val connector = new EmailConnector("textual/spam/corpus600.zip")
    //    val caseBase = new CachedLinealCaseBase()
    //
    //    // preCycle
    //    caseBase.init(connector)
    //    val cases = caseBase.getCases
    //
    //    OpennlpSplitter.split(cases)
    //    StopWordsDetector.detectStopWords(cases)
    //    TextStemmer.stem(cases)
    //
    //    // cycle

    //def createCase()

    //    val connector = new EmailConnector("textual/spam/corpus600.zip")
    //    val caseBase = new CachedLinealCaseBase()
    //    caseBase.init(connector)
    //    connector.retrieveAllCases()
    //    val config = new KNNClassificationConfig()
    //    config.setClassificationMethod(new MajorityVotingMethod())
    //    config.setK(3)

    val mlr = MultipleLinearRegression()
    val gain = InfoGainTransformer()

    // Construct the pipeline
    val pipeline = gain
    //.chainPredictor(mlr)
    //    val pipeline = gain

    // Train the pipeline (scaler and multiple linear regression)
    pipeline.fit(dataSet)
    val r: DataSet[LabeledVector] = pipeline.transform(dataSet)
    r print
  }
}

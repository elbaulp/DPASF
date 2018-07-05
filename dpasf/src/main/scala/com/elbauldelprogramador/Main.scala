package com.elbauldelprogramador

object Main {

  def main(args: Array[String]) {
    //    val env = ExecutionEnvironment.createLocalEnvironment(1)
    //    env.setParallelism(1)
    //    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    //    //      3, // number of restart attempts
    //    //      Time.of(10, TimeUnit.SECONDS) // delay
    //    //    ))
    //    //
    //    val data = (1 to 5).map(_ => Seq(Random.nextDouble, Random.nextDouble, Random.nextInt))
    //    val dataSet = env.fromCollection(data map { tuple =>
    //      val list = tuple.iterator.toList
    //      val numList = list map (_.asInstanceOf[Double])
    //      LabeledVector(numList(2), DenseVector(numList.take(2).toArray))
    //    })
    //
    //    val ida = IDADiscretizer(nAttrs = 2)
    //    val r = ida.discretize(dataSet)
    //    //val rr = r map (x => x.map(_.peekLast))
    //
    //    //    r print
    //
  }
}

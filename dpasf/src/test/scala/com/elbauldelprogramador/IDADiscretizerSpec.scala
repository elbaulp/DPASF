import com.elbauldelprogramador.discretizers.IDADiscretizer
import java.util.concurrent.TimeUnit
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import scala.util.Random

object fixtures extends Serializable {
  //val env = ExecutionEnvironment.getExecutionEnvironment
  val env = ExecutionEnvironment.createLocalEnvironment()
  env.setParallelism(1)
  //  env.getConfig.enableObjectReuse
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    3, // number of restart attempts
    Time.of(10, TimeUnit.SECONDS) // delay
  ))

  val data = env.readCsvFile[Iris](getClass.getResource("/iris2.dat").getPath)
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
  //  val dataSet = env.readCsvFile[ElecNormNew](
  //    getClass.getResource("/elecNormNew.arff").getPath,
  //    pojoFields = Array("date", "day", "period", "nswprice", "nswdemand", "vicprice", "vicdemand", "transfer", "label"))
}

// BDD tests
class IDADiscretizerSpec extends BddSpec with Serializable {
  import fixtures._
  "A Category" - {
    "When calling its Identity" - {
      "Should be computed correctly" in {
        val a = IDADiscretizer()
          .setBins(5)
          .setNumAttr(4)

        val discretized = a.discretize(dataSet)
        val cuts = a.cutPoints(dataSet)
        val discretized2 = a discretizeWith (cuts, dataSet)
        assert(discretized.collect.last === discretized2.collect.last)
      }
    }
    "When composing it" - {
      "Should be associative" in {
        //assert(Category.compose(Category.compose(f, g), h)(1) ==
        //  Category.compose(f, Category.compose(g, h))(1))
      }
    }
  }
}

// Thanks to http://blog.ssanj.net/posts/2016-07-06-how-to-run-scalacheck-from-scalatest-and-generate-html-reports.html
// for help me use scalacheck from scalatest
//class CategoryPropSpec extends CheckSpec {
//  import fixtures._
//
//  property("a == Id(a)") {
//    check(forAll { i:String =>
//      Category.Id(i) === i
//    })
//  }
//
//  property("Id∘f = f") {
//    check(forAll { i: Int =>
//      Category.Id(square(i)) === square(i)
//    })
//  }
//
//  property("f∘Id = f") {
//    check(forAll { i: Int =>
//      f(Category.Id(i)) === f(i)
//    })
//  }
//
//  property("Associativity: h∘(g∘f) = (h∘g)∘f = h∘g∘f"){
//    check(forAll { i: Int =>
//      Category.compose(Category.compose(f, g), h)(i) === Category.compose(f, Category.compose(g, h))(i)
//    })
//  }
//}

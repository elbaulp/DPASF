import com.elbauldelprogramador.discretizers.IDADiscretizer
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

object fixtures {
  val env = ExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val data = env.readCsvFile[Iris](getClass.getResource("/iris.dat").getPath)
  //val dataSet = new ArffFileStream(getClass.getResource("/elecNormNew.arff").getPath, -1)
  //val data = (0 to 10).map(_ => Seq(Random.nextDouble, Random.nextDouble, Random.nextString(3)))
  //val data1 = env.fromCollection(data)
  val dataSet = data
    .map { tuple =>
      val list = tuple.productIterator.toList
      val numList = list map (_.asInstanceOf[Double])
      LabeledVector(numList(4), DenseVector(numList.take(4).toArray))
    }
  //  val dataSet = env.readCsvFile[ElecNormNew](
  //    getClass.getResource("/elecNormNew.arff").getPath,
  //    pojoFields = Array("date", "day", "period", "nswprice", "nswdemand", "vicprice", "vicdemand", "transfer", "label"))
}

// BDD tests
class IDADiscretizerSpec extends BddSpec {
  import fixtures._
  "A Category" - {
    "When calling its Identity" - {
      "Should be computed correctly" in {
        val a = IDADiscretizer(nAttrs = 4)
        val r = a.discretize(dataSet)
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

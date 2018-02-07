import org.apache.flink.api.scala._

// Iris POJO
case class Iris(
  SepalLength: Double,
  SepalWidth: Double,
  PetalLength: Double,
  PetalWidth: Double,
  Class: Int)

object fixtures {
  val env = ExecutionEnvironment.getExecutionEnvironment
  //val dataSet = env.readCsvFile[Iris](getClass.getResource("/iris.dat").getPath)
  val dataSet = env.fromElements(1 to 10000 by 1)
}

// BDD tests
class IDADiscretizerSpec extends BddSpec {
  import fixtures._
  "A Category" - {
    "When calling its Identity" - {
      "Should be computed correctly" in {
        //assert(Category.Id(10) == 10)
        dataSet.print()
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

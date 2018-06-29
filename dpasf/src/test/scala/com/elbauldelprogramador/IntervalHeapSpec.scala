//import org.scalacheck.Prop._
//
//object IHfixtures {
//  import com.elbauldelprogramador.discretizers.IntervalHeap
//  import scala.collection.immutable.Range
//  import scala.util.Random
//  val ih = IntervalHeap(5, 1, 10)
//
//  (0 to 100).map(_ => ih.insert(Random.nextDouble))
//
//}
//
//class IntervalHeapSpec extends CheckSpec {
//  import IHfixtures._
//
//  property("a == Id(a)") {
//    check(forAll { i: Double =>
//      println(ih.nInstances)
//      ih.insert(i)
//      println(ih)
//      1 == 1
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
//  property("Associativity: h∘(g∘f) = (h∘g)∘f = h∘g∘f") {
//    check(forAll { i: Int =>
//      Category.compose(Category.compose(f, g), h)(i) === Category.compose(f, Category.compose(g, h))(i)
//    })
//  }
//}

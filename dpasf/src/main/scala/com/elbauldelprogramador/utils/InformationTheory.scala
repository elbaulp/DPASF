package com.elbauldelprogramador.utils

/**
 * Object containing utility functions for Information Theory
 */
object InformationTheory {

  private[this] val log2 = math.log(2)

  private[this] def log2(a: Double): Double = math.log(a) / log2

  private[this] def log(x: Double): Double =
    if (x > 0) math.log(x)
    else 0

  private[this] val nlogn = (x:Double) => x * log(x)
  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   * @param n     Number of elements
   *
   */
  private[this] def entropy(freqs: Seq[Double], n: Double) = {
    freqs.aggregate(0.0)({
      case (h, q) =>
        h + (if (q == 0) 0 else (q.toDouble / n) * (math.log(q.toDouble / n) / math.log(2)))
    }, { case (h1, h2) => h1 + h2 }) * -1
  }

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   */
  def entropy(freqs: Seq[Double]): Double =
    entropy(freqs, freqs.sum)

  /**
    * Computes conditional entropy of the columns given
    * the rows.
    *
    * @param freqs the contingency table
    * @return the conditional entropy of the columns given the rows
    */
  def entropyConditionedOnRows(freqs: Seq[Seq[Double]]): Double = {
    val total = freqs.map(_.sum).sum
    -freqs.aggregate(.0)({ case (h, q) =>
      (h + q.map(nlogn).sum) - nlogn(q.sum)
    },{ case (h1, h2) =>
      h1 + h2
    }) / (total * log2)
  }

  /**
    * Test using Fayyad and Irani's MDL criterion.
    *
    * @param priorCounts
    * @param bestCounts
    * @param numInstances
    * @param numCutPoints
    *
    * @return true if the splits is acceptable
    */
  def FayyadAndIranisMDL(priorCounts: Map[Int, Double],
                         bestCounts: Seq[Seq[Double]],
                         numInstances: Double,
                         numCutPoints: Int): Boolean = {
    // Entropy before split
    val priorH = entropy(priorCounts.values.toSeq)

    // Entropy after split
    val h = entropyConditionedOnRows(bestCounts)

    // Compute InfoGain
    val gain = priorH - h
//    assert(gain != priorH)

    // Number of classes occuring in the set
    val nClasses = priorCounts.keys.size

    // Number of classes in the left subset
    val nClassesLeft = bestCounts.head.count(_ != 0)
    // Number of classes in the right subset
    val nClassesRight = bestCounts(1).count(_ != 0)

    // Entropies for left and right
    val hLeft = entropy(bestCounts.head)
    val hRight = entropy(bestCounts(1))

    // MDL formula
    val delta = log2(math.pow(3, nClasses) - 2) -
      ((nClasses * priorH) - (nClassesRight * hRight) - (nClassesLeft * hLeft))

    // Check if split is accepted or not
//    gain > ((log2(numInstances - 1) + delta) / numInstances)
    gain > ((log2(numCutPoints - 1) + delta) / numInstances)
  }
}

package com.elbauldelprogramador.utils

/**
 * Object containing utility functions for Information Theory
 */
object InformationTheory {

  private[this] val log2 = math.log(2)
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
}
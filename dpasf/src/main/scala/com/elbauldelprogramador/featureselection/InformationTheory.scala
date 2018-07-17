package com.elbauldelprogramador.featureselection

/**
 * Object containing utility functions for Informatin Theory
 */
private[featureselection] object InformationTheory {

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

}
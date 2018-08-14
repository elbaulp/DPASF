package com.elbauldelprogramador.datastructures

import com.elbauldelprogramador.BddSpec

private object fixture {
  val nRows = 4
  val nCols = 200
  val min = 0
  val step = 0.005

  import java.util

  val m_CutPointsL1 = new util.ArrayList[Array[Double]](nRows)
  var i = 0
  while ({
    i < nRows
  }) {
    val initialb = new Array[Double](nCols + 1)
    var j = 0
    while ({
      j < nCols + 1
    }) {
      initialb(j) = min + j * step

      {
        j += 1; j - 1
      }
    }

    m_CutPointsL1.add(initialb)

    {
      i += 1; i - 1
    }
  }
}

class HistogramSpec extends BddSpec {
  import fixture._

  "A Histogram" - {
    "When creating CutPoints Matrix /w step = 0.005" - {
      "Should return the correct initial cutpoints" in {
        val h = Histogram(nRows, nCols, min, step)
        val buf = Array.fill(nCols + 1)(0d)
        for (row <- 0 until 1)
          for (col <- 0 to nCols)
            buf.update(col, h.cuts(row, col))

        assert(m_CutPointsL1.get(0) === buf)
      }
    }
  }

}

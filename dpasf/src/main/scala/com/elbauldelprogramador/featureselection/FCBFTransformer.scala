/*
 * Copyright (C) 2018  Alejandro Alcalde
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.elbauldelprogramador.featureselection

import com.elbauldelprogramador.utils.{ FlinkUtils, InformationTheory }
import org.apache.flink.api.scala.{ DataSet, _ }
import org.apache.flink.ml.common.{ LabeledVector, Parameter, ParameterMap }
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.pipeline.{ FitOperation, TransformDataSetOperation, Transformer }
import org.slf4j.LoggerFactory

import collection.mutable

/**
 * Fast Correlation-Based Filter (FCBF) algorithm as described in
 * Feature Selection for High-Dimensional Data: A Fast Correlation-Based
 * Filter Solution. Yu & Liu (ICML 2003)
 */
class FCBFTransformer extends Transformer[FCBFTransformer] {

  import FCBFTransformer._

  private[featureselection] var metricsOption: Option[Seq[Int]] = None

  /**
   * Sets the SU threshold. A value in [0,1) used as a threshold
   * for selecting the relevant features.
   *
   * @param thr Desired threshold
   * @return [[FCBFTransformer]]
   */
  def setThreshold(thr: Double): FCBFTransformer = {
    parameters add (Threshold, thr)
    this
  }
}

/**
 * Companion object of FCBF. Contains convenience functions and the parameter
 * type definitions of the algorithm
 */
object FCBFTransformer {

  private[this] val log = LoggerFactory.getLogger(this.getClass)

  private[this] val cache = mutable.Map.empty[(Int, Int), Double]

  // ====================================== Parameters =============================================
  private[FCBFTransformer] case object Threshold extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(.05)
  }

  // ==================================== Factory methods ==========================================
  def apply(): FCBFTransformer = new FCBFTransformer

  // ========================================== Operations =========================================

  /**
   * Trains the [[FCBFTransformer]] by extracting the most important features using
   * Symmetrical Uncertainty criterion. It opperates on a [[DataSet]] of type [[LabeledVector]]
   */
  implicit val fitLabeledVectorFCBF = new FitOperation[FCBFTransformer, LabeledVector] {
    override def fit(
      instance: FCBFTransformer,
      fitParameters: ParameterMap,
      input: DataSet[LabeledVector]): Unit = {

      val resultingParameters = instance.parameters ++ fitParameters
      val thr = resultingParameters(Threshold)
      log.info(s"Threshold set to $thr")

      val nAttrs = FlinkUtils.numAttrs(input)
      log.info(s"Reading Dataset with $nAttrs attrs")

      // Phase 1, calculate SU (symmetrical_uncertainty) for each Feature
      // w.r.t the class (C-Correlation)
      val su = for (i ← 0 until nAttrs) yield {
        val attr = input
          .map(lv ⇒ (lv.label, lv.vector(i)))
          .name("SU calculation")

        InformationTheory.symmetricalUncertainty(attr)
      }

      // SU ordered desc per SU_{i, c}
      // its a tuple with (su value, original index before sorting, flag indicating
      // feauture has been selected)
      val suSorted = su
        .zipWithIndex
        .filter(_._1 > thr)
        .sortBy(-_._1)
        .map { case (suValue, idx) ⇒ (suValue, idx, 1) }

      // Phase 2. Compute best features
      // Get first element
      val p = firstElement(suSorted)
      val sBest = for (_ ← 0 until nAttrs) yield {
        p match {
          case Some((psu, poidx, nextIdx)) ⇒
            val qtuple = nextElement(suSorted, nextIdx)
            val bestF = fcbf(p, qtuple, input)(suSorted)
            bestF.filter(_._3 == 1)
          case None ⇒ suSorted
        }
      }
      log.info(s"Best Features found: $sBest.last")
      val bestIndexs = sBest.last map (_._2)

      instance.metricsOption = Some(bestIndexs)
    }
  }

  /**
   * Returns tuple corresponding to first 'unconsidered' feature.
   *
   * @param s [[Seq]] with SU value, original feature index and column flag
   * @return [[Tuple3]] (Su value, original feature index, index of next 'unconsidered' feature)
   */
  private[this] def firstElement(s: Seq[(Double, Int, Int)]): Option[(Double, Int, Int)] = {
    val first = s.find(_._3 == 1)
    first.map(x ⇒ (x._1, x._2, s.indexWhere(_._1 == x._1)))
  }

  /**
   * Returns the tuple corresponding to the next 'unconsidered' feature.
   *
   * @param s   [[Seq]] with SU value, original feature index and column flag
   * @param idx original index of a feature whose next element is required.
   * @return [[Tuple3]] (Su value, original feature index, index of next 'unconsidered' feature)
   */
  private[this] def nextElement(s: Seq[(Double, Int, Int)], idx: Int): Option[(Double, Int, Int)] = {
    val next = s.zipWithIndex.find { case (x, oidx) ⇒ x._3 == 1 && oidx > idx }
    next.map { case (x, _) ⇒ (x._1, x._2, s.indexWhere(_._1 == x._1)) }
  }

  /**
   * Extracts the best features based on the Symmetrical Uncertainty computed as
   * a first step.
   *
   * It starts with the first element of =slist=, which is the element with greatest SU (F_p).
   * For all the remaining features (From the one right next to the current one to the
   * last one in the list), if F_p is a redundant peer to a feature F_q, F_q is removed
   * from the list. After one round of filtering features based on F_p, the algorithm will take
   * the currently remaining feature right next to F_p as the new reference to repeat the filtering
   * process.
   *
   * It stops when there is no more features to be removed from the list.
   *
   * @param p     feature being analyzed
   * @param q     feature right next to p
   * @param input [[DataSet]] with the features
   * @param slist List with information for each feature:
   *              (SU value, Original Index and flag indicaing if it's important)
   * @return Updated list with the selected features
   */
  private[this] def fcbf(
    p: Option[(Double, Int, Int)],
    q: Option[(Double, Int, Int)],
    input: DataSet[LabeledVector])(slist: Seq[(Double, Int, Int)]): Seq[(Double, Int, Int)] =
    p match {
      case Some((psu, pidx, pNextIdx)) ⇒
        q match {
          case Some((qsu, qidx, qNextIdx)) ⇒
            // TODO: Cache SU value for (p,q)?
            val pqSu = if (cache.isDefinedAt((pidx, qidx))) {
              log.debug(s"SU: Cache Hit for ($pidx, $qidx)")
              cache((pidx, qidx))
            } else {
              val su = InformationTheory.symmetricalUncertainty(
                input
                  map (x ⇒ (x vector qidx, x vector pidx))
                  name "PQ SU")
              cache((pidx, qidx)) = su
              su
            }
            val newSlist =
              if (pqSu >= qsu) slist.updated(qNextIdx, (qsu, qidx, 0))
              else slist
            // get Next element
            val nextq = nextElement(newSlist, qNextIdx)
            fcbf(p, nextq, input)(newSlist)
          case None ⇒
            // Get next element
            val nextp = nextElement(slist, pNextIdx)
            fcbf(nextp, q, input)(slist)
        }
      case None ⇒
        // No more elements to process, slist hast best features
        slist
    }

  implicit val transformDataSetLabeledVectorsFCBF = {
    new TransformDataSetOperation[FCBFTransformer, LabeledVector, LabeledVector] {
      override def transformDataSet(
        instance: FCBFTransformer,
        transformParameters: ParameterMap,
        input: DataSet[LabeledVector]): DataSet[LabeledVector] = {

        instance.metricsOption match {
          case Some(bests) ⇒
            input.map { x ⇒
              val attrs = x.vector
              val output = bests.map(attrs(_))
              LabeledVector(x.label, DenseVector(output.toArray))
            }
          case None ⇒
            throw new RuntimeException("The FCBF algorithm has not been fitted to the data. " +
              "This is necessary to compute the best features to select.")
        }
      }
    }
  }
}

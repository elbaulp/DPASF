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

package com.elbauldelprogramador.utils

import org.slf4j.LoggerFactory

object Utils {

  private[this] val log = LoggerFactory.getLogger(getClass.getSimpleName)

  def printToFile(f: java.io.File)(op: java.io.PrintWriter ⇒ Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def writeToFile(p: String, s: String): Unit = {
    val pw = new java.io.FileWriter(p, true)
    try pw.write(s"$s\n") finally pw.close()
  }

  def time[R](desc: String, path: String)(block: ⇒ R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val execTime = (t1 - t0) / 1e9
    log.info(s"$execTime for $desc")
    writeToFile(s"$path/$desc", execTime.toString)
    //    println(s"Elapsed time for $desc: " + (t1 - t0) + " ns")
    result
  }
}

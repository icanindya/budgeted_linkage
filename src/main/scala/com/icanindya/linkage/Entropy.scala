package com.icanindya.linkage

import sun.security.util.Length

object Entropy {
  def main(args: Array[String]): Unit = {
    val counts = Array.fill[Int](100000)(1)
    println(getValue(counts))
  }
  
  def getValue(counts: Array[Int]): Double = {
    val probs = counts.map(_.toDouble/counts.sum)
    val entropy = -1 * probs.filter(_ != 0).map(p => p * (Math.log(p)/Math.log(2))).sum
    entropy  
  }
  
}
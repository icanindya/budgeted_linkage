package com.icanindya.linkage

import sun.security.util.Length

object Entropy {
  def main(args: Array[String]): Unit = {
    val counts = Array(1, 1, 1, 1, 1, 1, 1)
    println(getValue(counts))
  }
  
  def getValue(counts: Array[Int]): Double = {
    val probs = counts.map(_.toDouble/counts.sum)
    val sumTerms = for(i <- 0 to probs.length - 1) yield if(probs(i) == 0) 0 else probs(i) * (Math.log(probs(i))/Math.log(2))
    val entropy = -1 * sumTerms.sum
    entropy  
  }
}
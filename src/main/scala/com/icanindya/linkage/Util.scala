package com.icanindya.linkage

import scala.math

object Util {
  
  def mean(values: Array[Double]): Double = {
    values.sum/values.length
  }
  
  def variance(values: Array[Double]): Double = {
    val mean = Util.mean(values)
    values.map(x => math.pow(x - mean, 2)).sum/values.length
  }
  
  def stdDeviation(values: Array[Double]): Double = {
    math.sqrt(variance(values))
  }
  
  
}
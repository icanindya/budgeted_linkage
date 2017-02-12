package com.icanindya.linkage

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Spark {
  
  def getContext(): SparkContext = {
    System.setProperty("hadoop.home.dir", "C:/hadoop241/");
    val conf = new SparkConf().setAppName("linkage").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "13g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    sc
  }
}
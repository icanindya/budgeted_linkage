package com.icanindya.linkage.migration

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object NC14Processor {
  
  val ncDataFile = "E:/Data/Linkage/NC/NC14/data/"
  val ncCorruptedDataFile = "E:/Data/Linkage/NC/NC14/corrupted/"
  val colSep = ","
  val FIRST_NAME = 3
  val LAST_NAME = 5
  val MIDDLE_NAME = 4
  val AGE = 7
  val SEX = 8
  val RACE = 9
  val ETHNICITY = 10
  val PHONE = 15
  
  val validSexes = List("", "m", "f", "u")
  val validRaces = List("", "a", "b", "i", "m", "o", "u", "w")
  val validEthinicities = List("", "hl", "nl", "un")
  
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/hadoop241/");
    val logger = Logger.getRootLogger
    logger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("linkage").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "10g")
    val sc = new SparkContext(conf)

    val data = sc.textFile(ncDataFile);
    
    val corruptedData = data.filter { line =>
     val tokens = line.split(colSep, -1).map(_.trim())
     val sex = tokens(SEX)
     val race = tokens(RACE)
     val ethinicity = tokens(ETHNICITY)
     val age = tokens(AGE)
     
     !(validSexes.contains(sex) && validRaces.contains(race) && validEthinicities.contains(ethinicity) && isValidAge(age))
    
    }
    corruptedData.coalesce(1, true).saveAsTextFile(ncCorruptedDataFile)
    
    println("Total lines: %s".format(data.count()))
    println("Corrupted lines: %s".format(corruptedData.count()))
    println("Percentage: %s%%".format((corruptedData.count() * 100.0)/data.count()))
//    data.collect().foreach { line => 
//      
//      val tokens  = line.split(colSep)
//      val sex = tokens(SEX)
//      val race = tokens(RACE)
//      
//      if(sex == "09/17/2008") println(line)
//      if(race == "\"\"4708/29/2008\"46/3 \"46f\" eth city #1\"462") println(line)
//    
//    }
  }
  
  def isValidAge(x: String): Boolean = x.isEmpty() || (x.toInt >= 16 && x.toInt <= 119) 
  
}
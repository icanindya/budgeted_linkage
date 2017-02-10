package com.icanindya.linkage.migration

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Calendar
import java.io.PrintWriter
import java.io.FileWriter
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Migration {

  val ncProcDataFile = "E:/Data/Linkage/NC/NC10/processed"
  val flProcDataFile = "E:/Data/Linkage/FL/31JUL2016/processed"
  val ncJoinFlDataFile = "E:/Data/Linkage/NC_JOIN_FL/data"
  val resultDir = "E:/Data/Linkage/NC_JOIN_FL/result_"
  val resultFile = "E:/Data/Linkage/NC_JOIN_FL/result"
  
  val sep = "\t"
  
  val NA_SX_AG_RC_PR_PHNB = 0
  val NA_SX_AG_RC_PR_PHAB = 1
  val NA_SX_AG_RC_PR = 2
  val NA_SX_AG_RC = 3 
  val NA_SX_AG = 4
  
  var OPTION = NA_SX_AG_RC_PR_PHAB
  val NUM_OPTIONS = 5
  
  val migSize = Array.ofDim[String](5)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/hadoop241/");
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("linkage").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "10g")
    val sc = new SparkContext(conf)

    val ncProcData = sc.textFile(ncProcDataFile)
    val flProcData = sc.textFile(flProcDataFile)
   
    val startTime = Calendar.getInstance.getTime
    
    val pw = new PrintWriter(new FileWriter(resultFile))
   
    for(OPTION <- 0 to NUM_OPTIONS - 1){
      this.OPTION = OPTION
      val ncDesData  = getDesiredData(ncProcData)
      val flDesData = getDesiredData(flProcData)
      val ncJoinFl = ncDesData.map((_, null)).join(flDesData.map((_, null))).map(_._1)
      
      val distOfdist = ncJoinFl.map((_, 1)).reduceByKey(_ + _).map(x => (x._2, 1)).reduceByKey(_+_).sortBy(_._2, false, 1)
      
      distOfdist.saveAsTextFile("E:/dist_of_dist_%d".format(OPTION))
      
      ncJoinFl.saveAsTextFile(ncJoinFlDataFile)
      
      migSize(OPTION) = "OP %d: total size: %s, tistinct size: %s".format(OPTION, ncJoinFl.count(), ncJoinFl.distinct().count())
      
      println(migSize(OPTION))
      pw.println(migSize(OPTION))
      pw.flush()
    }
    
    val endTime = Calendar.getInstance.getTime

    pw.println("Start time: %s\n, End time: %s".format(startTime, endTime))
    pw.close()
    
  }
  
  def getDesiredData(data: RDD[String]): RDD[String] = {
    var desData = data.map{ line =>
      val tokens = line.split(sep, -1)
      val fn = tokens(0)
      val mn = tokens(1)
      val ln = tokens(2)
      val sex = tokens(3)
      val age = tokens(4)
      val race = tokens(5)
      val party = tokens(6)
      val phone = tokens(7)
    
      if(OPTION == NA_SX_AG_RC_PR_PHNB || OPTION == NA_SX_AG_RC_PR_PHAB) "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(fn, mn, ln, sex, age, race, party, phone)
      else if(OPTION == NA_SX_AG_RC_PR) "%s\t%s\t%s\t%s\t%s\t%s\t%s".format(fn, mn, ln, sex, age, race, party)
      else if(OPTION == NA_SX_AG_RC) "%s\t%s\t%s\t%s\t%s\t%s".format(fn, mn, ln, sex, age, race)
      else if(OPTION == NA_SX_AG) "%s\t%s\t%s\t%s\t%s".format(fn, mn, ln, sex, age)
      else null
    }
    
    if(OPTION == NA_SX_AG_RC_PR_PHNB) desData = desData.filter(!_.split(sep, -1).last.isEmpty())
    desData
  }
  
}
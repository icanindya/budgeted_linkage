package com.icanindya.linkage

import scala.io.Source
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.FileWriter
import scala.collection.mutable.LinkedHashMap
import shapeless._
import scala.util.Random
import scala.Boolean
import scala.collection.mutable.ListBuffer
import java.io.File
import org.apache.spark.rdd.RDD

object Heuristics {
  
  val SCENE_NC = 0
  val SCENE_FL = 1
  
  val dsAttrs: Array[List[String]] = null
  val attrIndex: Map[String, Int] = null
  
//  
//  
//  
//  
//  val scene = SCENE_NC
//  
//  
//  
//  def main(args: Array[String]): Unit = {
//
//    if (scene == SCENE_NC) {
//      dsAttrs = NC_Extractor.dsAttrs
//      attrIndex = NC_Extractor.attrIndex
//      targetAttribute = NC_Extractor.TARGET_ATTRIBUTE
//      origDsPathFormat = NC_Extractor.ORIG_DATASET_PATH_FORMAT
//      distDsPathFormat = NC_Extractor.DIST_DATASET_PATH_FORMAT
//      allPaths = NC_Extractor.getAllPaths(0, 5)
//      normalJoinResultPath = NC_Extractor.NORMALJOIN_RESULT_PATH
//      blockJoinResultPath = NC_Extractor.BLOCKJOIN_RESULT_PATH
//      probabilisticJoinResultPath = NC_Extractor.PROBABILISTICJOIN_RESULT_PATH
//
//    } else if (scene == SCENE_FL) {
//      dsAttrs = FL_Extractor.dsAttrs
//      attrIndex = FL_Extractor.attrIndex
//      targetAttribute = FL_Extractor.TARGET_ATTRIBUTE
//      origDsPathFormat = FL_Extractor.ORIG_DATASET_PATH_FORMAT
//      distDsPathFormat = FL_Extractor.DIST_DATASET_PATH_FORMAT
//      allPaths = FL_Extractor.getAllPaths(0, 5)
//      normalJoinResultPath = FL_Extractor.NORMALJOIN_RESULT_PATH
//      blockJoinResultPath = FL_Extractor.BLOCKJOIN_RESULT_PATH
//      probabilisticJoinResultPath = FL_Extractor.PROBABILISTICJOIN_RESULT_PATH
//    }
//  }
//
//  def getHistogramDist(sc: SparkContext) {
//
//    val lines = sc.textFile(SAMP_FILE_PATH)
//    for (i <- 0 to attrIndex.size - 1) {
//      val valFreqs = lines.map(line => (line.split(COMMA)(i), 1))
//        .reduceByKey(_ + _)
//        .map(kv => Array(kv._1, kv._2).mkString(COMMA))
//        .collect()
//
//      val pw = new PrintWriter(new FileWriter(HISTOGRAM_FILE_PATH.format(i.toString()), false))
//      for (j <- 0 to valFreqs.length - 1) {
//        pw.println(valFreqs(j))
//      }
//      pw.close()
//    }
//
//  }
//
//  def getEntropies(sc: SparkContext) {
//    val entropiesAndVals = for (i <- 0 to attrIndex.size - 1) yield {
//      val counts = Source.fromFile(HISTOGRAM_FILE_PATH.format(i.toString())).getLines().map(_.split(COMMA)(1).trim().toLong).toArray
//      val sum = counts.sum
//      val entropy = -1 * counts.map { x =>
//        val p = x / sum.toDouble
//        p * (Math.log(p) / Math.log(2))
//      }.sum
//      val distinctVals = counts.length
//      "%f, %d".format(entropy, distinctVals)
//    }
//
//    val pw = new PrintWriter(new FileWriter(ENTROPY_FILE_PATH, false))
//    pw.println(entropiesAndVals.mkString(CRLF))
//    pw.close()
//  }
//
//  def getMasterFrequency(attr: String): Map[String, Double] = {
//    Source.fromFile(HISTOGRAM_FILE_PATH.format(attrIndex(attr).toString())).getLines()
//      .map { line =>
//        val kv = line.split(COMMA)
//        (kv(0), kv(1).toDouble)
//      }.toMap
//  }
//
//  def getMasterEntropy(attr: String): Double = {
//    val pairs = Source.fromFile(ENTROPY_FILE_PATH).getLines().map { line =>
//      val tokens = line.split(",").map(_.trim())
//      (tokens(0).toDouble, Entropy.getValue(Array.fill[Int](tokens(1).toInt)(1)))
//    }.toArray
//    //    println("pair: " + pairs.mkString(CRLF))
//
//    val entropies = pairs.map(_._1)
//    entropies(attrIndex(attr))
//  }
//
//  def estimateJoinSize(allPaths: List[Array[Int]]) {
//
//    for (path <- allPaths) {
//      //    val path = allPaths(2)
//
//      println("path: %s".format(path.mkString(",")))
//
//      var intermedFreqs = Map[String, Map[String, Double]]()
//      var attrSoFar = dsAttrs(path(1))
//      var joinSizeSoFar = datasetSizes(path(1))
//
//      for (attr <- attrIndex.keys) {
//        intermedFreqs += (attr -> getMasterFrequency(attr))
//      }
//      //      intermedFreqs = dummyFrequency
//
//      for (i <- 1 to path.length - 1) {
//        val newAttr = dsAttrs(path((i + 1) % path.length))
//        val newSize = datasetSizes((i + 1) % path.length)
//
//        var denoms = Map[String, Double]()
//
//        val size1 = joinSizeSoFar
//        val size2 = newSize
//
//        val commonAttr = attrSoFar.intersect(newAttr)
//        //        if(i == 2) println("attr so far: " + attrSoFar.mkString(","))
//        //        if(i == 2) println("common attr: " + commonAttr.mkString(","))
//
//        val productTerms = for (attr <- commonAttr) yield {
//          val frequency1 = intermedFreqs(attr)
//          val frequency2 = if ((i + 1) % path.length == 0) dummyFrequency(attr) else getMasterFrequency(attr)
//
//          val attrDomain = frequency1.keys.toList.union(frequency2.keys.toList)
//
//          val sumTerms = for (attrValue <- attrDomain) yield {
//            val count1 = if (frequency1.contains(attrValue)) frequency1(attrValue) else 0
//            val count2 = if (frequency2.contains(attrValue)) frequency2(attrValue) else 0
//            count1 * count2
//          }
//
//          val sum = sumTerms.sum
//          denoms += (attr -> sum)
//          val productTerm = sum.toDouble / (size1 * size2)
//          productTerm
//        }
//
//        val product = productTerms.product
//
//        val joinSize = size1 * size2 * product
//        //        if(i==4) println("joinSize: " + size2)
//
//        //        println("Common attributes %d - %d: %s".format(path(i), path((i+1)%path.length), commonAttr.mkString(", ")))
//        //        println("i: %d, join size: %f".format(i, joinSize))
//
//        for (attr <- commonAttr) {
//          val frequency1 = intermedFreqs(attr)
//          val frequency2 = getMasterFrequency(attr)
//
//          val attrDomain = frequency1.keys.toList.union(frequency2.keys.toList)
//
//          val joinFrequency = (for (attrValue <- attrDomain) yield {
//            val count1 = if (frequency1.contains(attrValue)) frequency1(attrValue) else 0
//            val count2 = if (frequency2.contains(attrValue)) frequency2(attrValue) else 0
//            (attrValue, (((count1 * count2) / denoms(attr)) * joinSize))
//          }).toMap
//
//          intermedFreqs += (attr -> joinFrequency)
//        }
//
//        val leftAttr = attrSoFar.diff(commonAttr)
//
//        for (attr <- leftAttr) {
//          val frequency = intermedFreqs(attr)
//          val attrDomain = frequency.keys
//          val joinFrequency = (for (attrValue <- attrDomain) yield {
//            (attrValue, (frequency(attrValue) * joinSize) / joinSizeSoFar)
//          }).toMap
//          intermedFreqs += (attr -> joinFrequency)
//        }
//
//        val rightAttr = dsAttrs(path((i + 1) % path.length)).diff(commonAttr)
//
//        for (attr <- rightAttr) {
//          val frequency = getMasterFrequency(attr)
//          val attrDomain = frequency.keys
//          val joinFrequency = (for (attrValue <- attrDomain) yield {
//            (attrValue, (frequency(attrValue) * joinSize) / joinSizeSoFar)
//          }).toMap
//          intermedFreqs += (attr -> joinFrequency)
//        }
//
//        attrSoFar = attrSoFar.union(dsAttrs(path((i + 1) % path.length)))
//        joinSizeSoFar = joinSize
//      }
//
//      println("Join size of path: %f".format(joinSizeSoFar))
//
//    }
//  }
//
//  def entroPath(allPaths: List[Array[Int]]) {
//    for (path <- allPaths) {
//
//      var intermedEntropies = Map[String, Double]()
//      for (attr <- attrIndex.keys) {
//        intermedEntropies += (attr -> getMasterEntropy(attr))
//      }
//
//      val sumTerms = for (i <- 1 to path.length - 2) yield {
//        val commonAttr = dsAttrs(path(i)).intersect(dsAttrs(path(i + 1)))
//        val entropies2 = for (attr <- commonAttr) yield {
//          getMasterEntropy(attr)
//        }
//        val entropies1 = if (i == 0) Array.fill[Double](commonAttr.length)(0.0).toList else entropies2
//        val entropies = for (i <- 0 to commonAttr.length - 1) yield {
//          Math.max(entropies1(i), entropies2(i))
//        }
//        entropies.sum
//      }
//
//      val sum = sumTerms.sum
//
//      println("sum: " + sum)
//    }
//  }
}
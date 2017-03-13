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
import com.icanindya.linkage.Util

object Heuristics {

  val SCENE_NC = 0
  val SCENE_FL = 1

  val COMMA = ","
  val CRLF = "\r\n"

  var dsAttrs: Array[Set[String]] = null
  var attrIndex: Map[String, Int] = null
  var targetAttribute: List[String] = null
  var origDsPathFormat: String = null
  var histFilePathFormat: String = null
  var entropyFilePath: String = null
  var allPaths: List[Array[Int]] = null
  var dataset: RDD[Array[String]] = null
  var dummyFrequency: Map[String, Map[String, Double]] = null

  val dsSize = 100000
  val datasetSizes = List(1.toDouble, dsSize.toDouble, dsSize.toDouble, dsSize.toDouble, dsSize.toDouble, dsSize.toDouble)

  val scene = SCENE_FL

  def main(args: Array[String]): Unit = {

    val sc = Spark.getContext()

    if (scene == SCENE_NC) {
      dsAttrs = NC_Extractor.dsAttrs
      attrIndex = NC_Extractor.attrIndex
      targetAttribute = NC_Extractor.TARGET_ATTRIBUTES
      origDsPathFormat = NC_Extractor.ORIG_DATASET_PATH_FORMAT
      histFilePathFormat = NC_Extractor.HISTOGRAM_FILE_PATH_FORMAT
      entropyFilePath = NC_Extractor.ENTROPY_FILE_PATH
      allPaths = NC_Extractor.getAllPaths()
      dummyFrequency = NC_Extractor.dummyFrequency

    } else if (scene == SCENE_FL) {
      dsAttrs = FL_Extractor.dsAttrs
      attrIndex = FL_Extractor.attrIndex
      targetAttribute = FL_Extractor.TARGET_ATTRIBUTES
      origDsPathFormat = FL_Extractor.ORIG_DATASET_PATH_FORMAT
      histFilePathFormat = FL_Extractor.HISTOGRAM_FILE_PATH_FORMAT
      entropyFilePath = FL_Extractor.ENTROPY_FILE_PATH
      allPaths = FL_Extractor.getAllPaths()
      dummyFrequency = FL_Extractor.dummyFrequency
    }

    dataset = sc.textFile(origDsPathFormat.format(dsSize)).map(_.split(COMMA, -1))
    //    generateMetadata(sc, dsSize)
//    showHeur1Scores(allPaths)
//    showRevHeur1Scores(allPaths)
    showHeur2Scores(allPaths)
    showHeur3Scores(allPaths)
  }

  def generateMetadata(sc: SparkContext, dsSize: Int) {

    generateHistogram(sc, dataset)
    generateEntropies(sc)

  }

  def generateHistogram(sc: SparkContext, dataset: RDD[Array[String]]) {
    for (i <- attrIndex.values) {
      val valFreqMap = dataset.map { tuple =>
        (tuple(i), 1)
      }
        .reduceByKey(_ + _)
        .collectAsMap()

      val pw = new PrintWriter(new FileWriter(histFilePathFormat.format(i), false))
      valFreqMap.foreach(kv => pw.println("%s,%d".format(kv._1, kv._2)))
      pw.close()
    }
  }

  def generateEntropies(sc: SparkContext) {
    val entropiesAndDistVals = attrIndex.values.map { i =>
      val counts = sc.textFile(histFilePathFormat.format(i)).map(_.split(COMMA, -1)).map(_(1).toInt).collect()
      (i, (Entropy.getValue(counts), counts.length))
    }.toSeq.sortWith((x, y) => x._1 < y._1).map(_._2)

    val pw = new PrintWriter(new FileWriter(entropyFilePath, false))
    entropiesAndDistVals.foreach(kv => pw.println("%f, %d".format(kv._1, kv._2)))
    pw.close()

  }

  def getMasterFrequency(attr: String): Map[String, Double] = {
    Source.fromFile(histFilePathFormat.format(attrIndex(attr))).getLines()
      .map { line =>
        val kv = line.split(COMMA, -1)
        (kv(0), kv(1).toDouble)
      }.toMap
  }

  def getMasterEntropy(attr: String): Double = {
    val pairs = Source.fromFile(entropyFilePath).getLines().map { line =>
      val tokens = line.split(COMMA, -1).map(_.trim())
      tokens(0).toDouble
    }.toArray

    val entropies = pairs
    entropies(attrIndex(attr))
  }

  def showHeur1Scores(allPaths: List[Array[Int]]) {
    val startTime = System.currentTimeMillis()

    for (path <- allPaths) {
      val pathStartTime = System.currentTimeMillis()
      
      println("path: %s".format(path.mkString(",")))
      
      var attrSoFar = dsAttrs(path(0))
      var joinSizeSoFar = 1.0
      
      var lAttrValFreqs = dummyFrequency
      
      val rAttrValFreqs = attrIndex.keys.map(attr => (attr, getMasterFrequency(attr))).toMap
     
      for (i <- 0 to path.length - 2) {

        val commonAttr = attrSoFar.intersect(dsAttrs(path(i + 1)))
        
        val lSize = joinSizeSoFar
        val rSize = dsSize
        
        val productTerm = commonAttr.map{attr =>
        
          val lValFreqs = lAttrValFreqs(attr)
          val rValFreqs = rAttrValFreqs(attr) 
          
          val lValues = lValFreqs.keys
          val rValues = rValFreqs.keys
          
          val commValues = lValues.toSet.intersect(rValues.toSet)
          
          val sumTerm = commValues.map(v => lValFreqs(v) * rValFreqs(v)).sum
          
          sumTerm /(lSize * rSize)
        }.product
        
        val joinSize = lSize * rSize * productTerm
        
        var tempAttrValFreqs = Map[String, Map[String, Double]]()
        
        commonAttr.map{attr =>
          val lValFreqs = lAttrValFreqs(attr)
          val rValFreqs = rAttrValFreqs(attr)
          
          val lValues = lValFreqs.keys
          val rValues = rValFreqs.keys
          
          val commValues = lValues.toSet.intersect(rValues.toSet)
          
          val sumTerm = commValues.map(v => lValFreqs(v) * rValFreqs(v)).sum
         
          val valFreqs = commValues.map{v =>
            val freq = joinSize * ((lValFreqs(v) * rValFreqs(v))/sumTerm)
            (v, freq)
          }.toMap
          
          tempAttrValFreqs += (attr -> valFreqs)
        }

        val leftAttr = attrSoFar.diff(commonAttr)

        leftAttr.map{attr =>
          val lValFreqs = lAttrValFreqs(attr)
          val lValues = lValFreqs.keys
          
          val valFreqs = lValues.map{v =>
            val freq = lValFreqs(v) * (joinSize/lSize)
            (v, freq)
          }.toMap
          
          tempAttrValFreqs += (attr -> valFreqs)
        }
        
        val rightAttr =  dsAttrs(path(i + 1)).diff(commonAttr)

        rightAttr.map{attr =>
          val rValFreqs = rAttrValFreqs(attr)
          val rValues = rValFreqs.keys
          
          val valFreqs = rValues.map{v =>
            val freq = rValFreqs(v) * (joinSize/rSize)
            (v, freq)
          }.toMap
          
          tempAttrValFreqs += (attr -> valFreqs)
        }
        
        
        lAttrValFreqs = tempAttrValFreqs
        attrSoFar = attrSoFar.union(dsAttrs(path((i + 1) % path.length)))
        joinSizeSoFar = joinSize
      }

      println("Join size: %f".format(joinSizeSoFar))
      
      val pathEndTime = System.currentTimeMillis()
      println("Path time: " + (pathEndTime - pathStartTime))

    }
    
    val endTime = System.currentTimeMillis()
    
    println("Total time: " + (endTime - startTime))
  }
  
  def showRevHeur1Scores(allPaths: List[Array[Int]]) {

    for (path <- allPaths) {
      println("path: %s".format(path.mkString(",")))
      
      var attrSoFar = dsAttrs(path(path.length - 1))
      var joinSizeSoFar = dsSize.toDouble
      
      var rAttrValFreqs = attrIndex.keys.map(attr => (attr, getMasterFrequency(attr))).toMap
      var lAttrValFreqs = attrIndex.keys.map(attr => (attr, getMasterFrequency(attr))).toMap
     
      for (i <- path.length - 1 to 1 by -1) {

        val commonAttr = attrSoFar.intersect(dsAttrs(path(i - 1)))
        
        val rSize = joinSizeSoFar
        var lSize = dsSize.toDouble
        
        if(i == 1){
          lAttrValFreqs = dummyFrequency
          lSize = 1.0
        }
        
        val productTerm = commonAttr.map{attr =>
        
          val lValFreqs = lAttrValFreqs(attr)
          val rValFreqs = rAttrValFreqs(attr) 
          
          val lValues = lValFreqs.keys
          val rValues = rValFreqs.keys
          
          val commValues = lValues.toSet.intersect(rValues.toSet)
          
          val sumTerm = commValues.map(v => lValFreqs(v) * rValFreqs(v)).sum
          
          sumTerm /(lSize * rSize)
        }.product
        
        val joinSize = lSize * rSize * productTerm
        
        var tempAttrValFreqs = Map[String, Map[String, Double]]()
        
        commonAttr.map{attr =>
          val lValFreqs = lAttrValFreqs(attr)
          val rValFreqs = rAttrValFreqs(attr)
          
          val lValues = lValFreqs.keys
          val rValues = rValFreqs.keys
          
          val commValues = lValues.toSet.intersect(rValues.toSet)
          
          val sumTerm = commValues.map(v => lValFreqs(v) * rValFreqs(v)).sum
         
          val valFreqs = commValues.map{v =>
            val freq = joinSize * ((lValFreqs(v) * rValFreqs(v))/sumTerm)
            (v, freq)
          }.toMap
          
          tempAttrValFreqs += (attr -> valFreqs)
        }

        val rightAttr = attrSoFar.diff(commonAttr)

        rightAttr.map{attr =>
          val rValFreqs = rAttrValFreqs(attr)
          val rValues = rValFreqs.keys
          
          val valFreqs = rValues.map{v =>
            val freq = rValFreqs(v) * (joinSize/rSize)
            (v, freq)
          }.toMap
          
          tempAttrValFreqs += (attr -> valFreqs)
        }
        
        val leftAttr =  dsAttrs(path(i - 1)).diff(commonAttr)

        leftAttr.map{attr =>
          val lValFreqs = lAttrValFreqs(attr)
          val lValues = lValFreqs.keys
          
          val valFreqs = lValues.map{v =>
            val freq = lValFreqs(v) * (joinSize/lSize)
            (v, freq)
          }.toMap
          
          tempAttrValFreqs += (attr -> valFreqs)
        }
        
        
        rAttrValFreqs = tempAttrValFreqs
        attrSoFar = attrSoFar.union(dsAttrs(path((i - 1) % path.length)))
        joinSizeSoFar = joinSize
      }

      println("Join size: %f".format(joinSizeSoFar))

    }
  }
  
  

  def showHeur2Scores(allPaths: List[Array[Int]]) {
    
    val startTime = System.currentTimeMillis()
    
    for (path <- allPaths) {

      println("path: %s".format(path.mkString(COMMA)))

      var attrSoFar = dsAttrs(path(0))

      val entropySums = for (i <- 0 to path.length - 2) yield {

        val commonAttr = attrSoFar.intersect(dsAttrs(path(i + 1))).toList

        val entropySum2 = commonAttr.map(getMasterEntropy(_)).sum

        attrSoFar = attrSoFar.union(dsAttrs(path(i + 1)))
        math.min(entropySum2, 16.60964047441802)
      }

      println("score: " + Util.mean(entropySums.toArray))

    }
    
    val endTime = System.currentTimeMillis()
    
    println("total time: " + (endTime - startTime))
  }

  def showHeur3Scores(allPaths: List[Array[Int]]) {
    val startTime = System.currentTimeMillis()
    
    val attrEntropies = attrIndex.keys.map(attr => (attr, getMasterEntropy(attr))).toMap
    val sumEntropies = attrEntropies.values.sum
    val attrWeights = attrEntropies.mapValues(_/sumEntropies)
    
    println(attrWeights)
    
    for (path <- allPaths) {

      println("path: %s".format(path.mkString(COMMA)))

      var attrSoFar = dsAttrs(path(0))

      val attributeSum = for (i <- 0 to path.length - 2) yield {
        val commonAttr = attrSoFar.intersect(dsAttrs(path(i + 1))).toList
        attrSoFar = attrSoFar.union(dsAttrs(path(i + 1)))
        commonAttr.map(attr => weight(attr)).sum
      }

      println("score: " + Util.mean(attributeSum.toArray))

    }
    
    val endTime = System.currentTimeMillis()
    println("total time: " + (endTime - startTime))
  }
  
  def weight(attr: String): Double = {
    
    if(List("voter_num", "nc_id", "address", "email").contains(attr)) return 5
    else if (List("first_name", "middle_name", "last_name", "phone", "reg_date", "dob").contains(attr)) return 2
    else if (List("zip", "county").contains(attr)) return 1
    else return 1
  }
  
//"first_name" -> 0,
//    "middle_name" -> 1,
//    "last_name" -> 2,
//    "sex" -> 3,
//    "race" -> 4,
//    "ethnicity" -> 5,
//    "age" -> 6,
//    "birth_place" -> 7,
//    "zip" -> 8,
//    "county" -> 9,
//    "party" -> 10,
//    "reg_date" -> 11,
//    "phone" -> 12,
//    "voter_num" -> 13,
//    "nc_id" -> 14,
//    "address" -> 15)
  
//"first_name" -> Map[String, Double](),
//    "middle_name" -> Map[String, Double](),
//    "last_name" -> Map[String, Double](),
//    "sex" -> Map("F" -> 1.0),
//    "race" -> Map("2" -> 1.0),
//    "dob" -> Map[String, Double](),
//    "zip" -> Map("34983" -> 1.0),
//    "county" -> Map("STL" -> 1.0),
//    "party" -> Map[String, Double](),
//    "reg_date" -> Map[String, Double](),
//    "phone" -> Map("9187" -> 1.0),
//    "voter_num" -> Map[String, Double](),
//    "email" -> Map[String, Double](),
//    "address" -> Map[String, Double]())
}


//def showHeur1Scores(allPaths: List[Array[Int]]) {
//
//    for (path <- allPaths) {
//      println("path: %s".format(path.mkString(COMMA)))
//
//      var intermedFreqs = Map[String, Map[String, Double]]()
//      var attrSoFar = dsAttrs(path(1))
//      var joinSizeSoFar = datasetSizes(path(1))
//
//      for (attr <- attrIndex.keys) {
//        intermedFreqs += (attr -> getMasterFrequency(attr))
//      }
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
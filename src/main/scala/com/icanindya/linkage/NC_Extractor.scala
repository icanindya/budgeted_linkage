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
import com.rockymadden.stringmetric.similarity._
import javax.xml.crypto.dsig.keyinfo.KeyValue
import java.io.File
import org.apache.spark.rdd.RDD

import org.apache.spark.sql._

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.sql.functions.concat_ws
import scala.collection.mutable.WrappedArray

import java.util.concurrent.TimeUnit
import org.codehaus.janino.Java.Lvalue

object NC_Extractor {
  // dataset address: https://s3.amazonaws.com/dl.ncsbe.gov/data/Snapshots/VR_Snapshot_20160315.zip

  val ORIG_FILE_PATH = "E:/Data/Linkage/NC/NC16/data"
  val SAMP_FILE_PATH = "E:/Data/Linkage/NC/NC16/sampled/%d_%d"
  val CSV_FILE_PATH = "E:/Data/Linkage/NC/NC16/sampled/csv/NC16_%d.csv"
  val DIST_DATASET_DIR = "E:/Data/Linkage/NC/NC16/sampled/csv/"
  val HISTOGRAM_FILE_PATH = "E:/Data/Linkage/NC/NC16/histograms/%s.txt"
  val ENTROPY_FILE_PATH = "E:/Data/Linkage/NC/NC16/entropies/entropy.txt"
  val CORRUPTED_FILE_PATH = "E:/Data/Linkage/NC/NC16/corrupted/nc_16_cor%s_%s.csv"

  val BLOCKJOIN_RESULT_PATH = "E:/Data/Linkage/NC/NC16/result/block_join_result.txt"
  val PROBABILISTICJOIN_RESULT_PATH = "E:/Data/Linkage/NC/NC16/result/probabilistic_join_result.txt"
  val NORMALJOIN_RESULT_PATH = "E:/Data/Linkage/NC/NC16/result/normal_join_result.txt"
  
  val ORIG_DATASET_PATH_FORMAT = DIST_DATASET_DIR + "NC16_%d_0.csv" //.format(dsSize)
  val DIST_DATASET_PATH_FORMAT = DIST_DATASET_DIR + "NC16_%d_%d_%d.csv" //.format(dsSize, corrLevel, i)

  val CASE_NORMAL_JOIN = 0
  val CASE_BLOCK_JOIN = 1
  val CASE_PROBABILISTIC_JOIN = 2

  val SEPERATOR = "\t"
  val CRLF = "\r\n"
  val COMMA = ","

  val NUM_DATASETS = 6
  val GRAM_SIZE = 3
  val NUM_TEST_SAMPLES = 500
  val NUM_HASH_TABLES = 5
  val DISTANCE_THRESHOLD = 0.3
  val LEVENSTEIN_THRESHOLD = 0
  val TARGET_ATTRIBUTE = "age"

  val NUM_CORRUPTED_ATTR = 7
  val NUM_ATTR = 15
  val levensteinThresPerAttr = NUM_CORRUPTED_ATTR / NUM_ATTR.toDouble

  // 0 based column indices for NC 16 attributes
  val COUNTY = 2
  val VOTER_NUM = 3
  val NC_ID = 4
  val LAST_NAME = 11
  val FIRST_NAME = 12
  val MIDDLE_NAME = 13
  val HOUSE_NUM = 15
  val STREET_NAME = 18
  val STREET_TYPE = 19
  val CITY = 23
  val STATE = 24
  val ZIP = 25
  val AREA_CODE = 33
  val PHONE = 34
  val RACE = 36
  val ETHNICITY = 38
  val PARTY = 39
  val SEX = 41
  val AGE = 43
  val BIRTH_PLACE = 44
  val REGISTRATION_DATE = 45

  val attrIndex = Map(
    "first_name" -> 0,
    "middle_name" -> 1,
    "last_name" -> 2,
    "sex" -> 3,
    "race" -> 4,
    "ethnicity" -> 5,
    "age" -> 6,
    "birth_place" -> 7,
    "zip" -> 8,
    "county" -> 9,
    "party" -> 10,
    "reg_date" -> 11,
    "phone" -> 12,
    "voter_num" -> 13,
    "nc_id" -> 14,
    "address" -> 15)

  val dummyFrequency = Map("first_name" -> Map("JOHN" -> 1.0),
    "middle_name" -> Map("DAVID" -> 1.0),
    "last_name" -> Map("HART" -> 1.0),
    "sex" -> Map("M" -> 1.0),
    "race" -> Map[String, Double](),
    "ethnicity" -> Map[String, Double](),
    "age" -> Map[String, Double](),
    "birth_place" -> Map("NC" -> 1.0),
    "zip" -> Map("27302" -> 1.0),
    "county" -> Map("ALAMANCE" -> 1.0),
    "party" -> Map[String, Double](),
    "reg_date" -> Map[String, Double](),
    "phone" -> Map("*******658" -> 1.0),
    "voter_num" -> Map[String, Double](),
    "nc_id" -> Map[String, Double](),
    "address" -> Map("2567, NC HWY 119, MEBANE, NC" -> 1.0))

  val dsAttrs = Array.ofDim[List[String]](NUM_DATASETS)

  dsAttrs(0) = List("first_name", "middle_name", "last_name", "sex", "address", "zip", "phone", "county", "birth_place")
  dsAttrs(1) = List("first_name", "middle_name", "reg_date", "zip", "race", "birth_place")
  dsAttrs(2) = List("last_name", "sex", "address", "voter_num")
  dsAttrs(3) = List("voter_num", "reg_date", "race", "ethnicity", "party")
  dsAttrs(4) = List("phone", "county", "nc_id")
  dsAttrs(5) = List("reg_date", "nc_id", "race", "ethnicity", "party", "age")

  /* ------------------------------------------------------------- */

  val datasetSizes = Array(1.0, 10000.0, 10000.0, 10000.0, 10000.0)

  var master, cor5D1, cor5D2, cor5D3, cor5D4, cor5D5, cor10D1, cor10D2, cor10D3, cor10D4, cor10D5 = List[Array[String]]()
  var cor5 = Array[List[Array[String]]]()
  var cor10 = Array[List[Array[String]]]()

  val datasetParams = List((1000, 4), (10000, 4), (100000, 4), (1000000, 4))
  val LEVENSHTEIN_DIST_THRES = 1

  /* ------------------------------------------------------------- */

  var pw = new PrintWriter(new FileWriter("E:/result.txt", false))
  def main(args: Array[String]): Unit = {

    //    processAndSample(sc, datasetParams)
    //    generateCSV(sc, datasetParams)

    //    getAllPaths(0, 5).foreach {x => println(x.mkString("-"))}

    val option = CASE_NORMAL_JOIN

    pw = new PrintWriter(new FileWriter(NORMALJOIN_RESULT_PATH, true))

    for (dsSize <- List(1000, 10000, 100000, 1000000)) { //List(1000, 10000, 100000, 1000000)
      for (path <- List(getAllPaths(0, 5)(0), getAllPaths(0, 5)(3))) {
        for (corrLevel <- List(0, 5, 10)) { // List(0, 5, 10)
          for (levensteinThres <- List(0, 1)) {
            val sc = Spark.getContext()
            println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))
            pw.println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))

            if (option == CASE_NORMAL_JOIN) {
              pw = new PrintWriter(new FileWriter(NORMALJOIN_RESULT_PATH, true))
              normalJoin(sc, pw, dsSize, path, corrLevel, levensteinThres)
              pw.flush
              pw.close
            } else if (option == CASE_PROBABILISTIC_JOIN) {
              pw = new PrintWriter(new FileWriter(PROBABILISTICJOIN_RESULT_PATH, true))
              probabilisticJoin(sc, pw, dsSize, path, corrLevel, levensteinThres)
              pw.flush
              pw.close
            } else if (option == CASE_BLOCK_JOIN) {
              pw = new PrintWriter(new FileWriter(BLOCKJOIN_RESULT_PATH, true))
              blockJoin(sc, pw, dsSize, path, corrLevel, levensteinThres)
              pw.flush
              pw.close
            }
            sc.stop()
          }
        }
      }
    }

  }
  
  def getAllPaths(s: Int, t: Int): List[Array[Int]] = {
    val graph = new Graph(new AdjacencyList(dsAttrs).get())
    graph.allPaths(s, t)
  }

  def processAndSample(sc: SparkContext, params: List[(Int, Int)]) {
    val lines = sc.textFile(ORIG_FILE_PATH)

    val filtered = lines.map { line =>
      val tokens = line.split(SEPERATOR, -1).map(_.trim())

      val first_name = tokens(FIRST_NAME)
      val middle_name = tokens(MIDDLE_NAME)
      val last_name = tokens(LAST_NAME)

      val sex = tokens(SEX)
      val race = tokens(RACE)
      val ethnicity = tokens(ETHNICITY)
      val age = tokens(AGE)
      val birth_place = tokens(BIRTH_PLACE)
      val zip = tokens(ZIP)
      val county = tokens(COUNTY)
      val party = tokens(PARTY)
      val regDate = tokens(REGISTRATION_DATE)
      val phone = tokens(AREA_CODE) + tokens(PHONE)
      val voterNum = tokens(VOTER_NUM)
      val ncId = tokens(NC_ID)

      val houseNum = if (tokens(HOUSE_NUM).isEmpty()) "" else tokens(HOUSE_NUM) + " "
      val streetName = if (tokens(STREET_NAME).isEmpty()) "" else tokens(STREET_NAME) + " "
      val streetType = if (tokens(STREET_TYPE).isEmpty()) "" else tokens(STREET_TYPE) + " "
      val city = if (tokens(CITY).isEmpty()) "" else tokens(CITY) + " "
      val state = tokens(STATE)

      val address = houseNum + streetName + streetType + city + state
      new NC_Person(first_name, middle_name, last_name, sex, race, ethnicity, age, birth_place, zip, county, party, regDate, phone, voterNum, ncId, address)
    }
      .filter { person =>
        !person.first_name.isEmpty() &&
          !person.middle_name.isEmpty() &&
          !person.last_name.isEmpty() &&
          !person.sex.isEmpty() &&
          !person.race.isEmpty() &&
          !person.ethnicity.isEmpty() &&
          !person.age.isEmpty() &&
          !person.birth_place.isEmpty() &&
          !person.zip.isEmpty() &&
          !person.county.isEmpty() &&
          !person.party.isEmpty() &&
          !person.regDate.isEmpty() &&
          person.phone.length() == 10 &&
          !person.voterNum.isEmpty() &&
          !person.ncId.isEmpty() &&
          !person.address.isEmpty() &&
          !person.address.endsWith(" ")
      }
      .map { person =>
        val attributes = Array(person.first_name, person.middle_name, person.last_name, person.sex, person.race, person.ethnicity, person.age, person.birth_place, person.zip, person.county, person.party, person.regDate, person.phone, person.voterNum, person.ncId, person.address)
        val str = attributes.mkString(COMMA)
        str
      }

    filtered.cache()

    for ((sampleSize, phDigits) <- params) {
      val sampled = filtered.takeSample(false, sampleSize, 121690)
      sc.parallelize(sampled).map { line =>
        val buffer = line.split(COMMA, -1).toBuffer
        val phone = buffer.remove((attrIndex("phone")))
        val sanitizedPh = if (phDigits <= phone.length()) phone.substring(phone.length() - phDigits) else phone
        buffer.insert(attrIndex("phone"), sanitizedPh)
        buffer.mkString(COMMA)
      }
        .coalesce(1).saveAsTextFile(SAMP_FILE_PATH.format(sampleSize, phDigits))
      println(sampled.length)
    }

  }

  def generateCSV(sc: SparkContext, params: List[(Int, Int)]) {
    // to be used with the ANU corrupter scripts

    for ((sampleSize, phDigits) <- params) {
      val lines = sc.textFile(SAMP_FILE_PATH.format(sampleSize, phDigits))
      val pw = new PrintWriter(new File(CSV_FILE_PATH.format(sampleSize, phDigits)))
      pw.println(attrIndex.toSeq.sortBy(_._2).map("\"" + _._1 + "\"").mkString(COMMA))
      for (line <- lines.map(_.split(COMMA, -1).map("\"" + _ + "\"").mkString(COMMA)).collect()) {
        pw.println(line)
      }
      pw.close
    }
  }

  

  def getHistogramDist(sc: SparkContext) {

    val lines = sc.textFile(SAMP_FILE_PATH)
    for (i <- 0 to attrIndex.size - 1) {
      val valFreqs = lines.map(line => (line.split(COMMA)(i), 1))
        .reduceByKey(_ + _)
        .map(kv => Array(kv._1, kv._2).mkString(COMMA))
        .collect()

      val pw = new PrintWriter(new FileWriter(HISTOGRAM_FILE_PATH.format(i.toString()), false))
      for (j <- 0 to valFreqs.length - 1) {
        pw.println(valFreqs(j))
      }
      pw.close()
    }

  }

  def getEntropies(sc: SparkContext) {
    val entropiesAndVals = for (i <- 0 to attrIndex.size - 1) yield {
      val counts = Source.fromFile(HISTOGRAM_FILE_PATH.format(i.toString())).getLines().map(_.split(COMMA)(1).trim().toLong).toArray
      val sum = counts.sum
      val entropy = -1 * counts.map { x =>
        val p = x / sum.toDouble
        p * (Math.log(p) / Math.log(2))
      }.sum
      val distinctVals = counts.length
      "%f, %d".format(entropy, distinctVals)
    }

    val pw = new PrintWriter(new FileWriter(ENTROPY_FILE_PATH, false))
    pw.println(entropiesAndVals.mkString(CRLF))
    pw.close()
  }

  def getMasterFrequency(attr: String): Map[String, Double] = {
    Source.fromFile(HISTOGRAM_FILE_PATH.format(attrIndex(attr).toString())).getLines()
      .map { line =>
        val kv = line.split(COMMA)
        (kv(0), kv(1).toDouble)
      }.toMap
  }

  def getMasterEntropy(attr: String): Double = {
    val pairs = Source.fromFile(ENTROPY_FILE_PATH).getLines().map { line =>
      val tokens = line.split(",").map(_.trim())
      (tokens(0).toDouble, Entropy.getValue(Array.fill[Int](tokens(1).toInt)(1)))
    }.toArray
    //    println("pair: " + pairs.mkString(CRLF))

    val entropies = pairs.map(_._1)
    entropies(attrIndex(attr))
  }

  def estimateJoinSize(allPaths: List[Array[Int]]) {

    for (path <- allPaths) {
      //    val path = allPaths(2)

      println("path: %s".format(path.mkString(",")))

      var intermedFreqs = Map[String, Map[String, Double]]()
      var attrSoFar = dsAttrs(path(1))
      var joinSizeSoFar = datasetSizes(path(1))

      for (attr <- attrIndex.keys) {
        intermedFreqs += (attr -> getMasterFrequency(attr))
      }
      //      intermedFreqs = dummyFrequency

      for (i <- 1 to path.length - 1) {
        val newAttr = dsAttrs(path((i + 1) % path.length))
        val newSize = datasetSizes((i + 1) % path.length)

        var denoms = Map[String, Double]()

        val size1 = joinSizeSoFar
        val size2 = newSize

        val commonAttr = attrSoFar.intersect(newAttr)
        //        if(i == 2) println("attr so far: " + attrSoFar.mkString(","))
        //        if(i == 2) println("common attr: " + commonAttr.mkString(","))

        val productTerms = for (attr <- commonAttr) yield {
          val frequency1 = intermedFreqs(attr)
          val frequency2 = if ((i + 1) % path.length == 0) dummyFrequency(attr) else getMasterFrequency(attr)

          val attrDomain = frequency1.keys.toList.union(frequency2.keys.toList)

          val sumTerms = for (attrValue <- attrDomain) yield {
            val count1 = if (frequency1.contains(attrValue)) frequency1(attrValue) else 0
            val count2 = if (frequency2.contains(attrValue)) frequency2(attrValue) else 0
            count1 * count2
          }

          val sum = sumTerms.sum
          denoms += (attr -> sum)
          val productTerm = sum.toDouble / (size1 * size2)
          productTerm
        }

        val product = productTerms.product

        val joinSize = size1 * size2 * product
        //        if(i==4) println("joinSize: " + size2)

        //        println("Common attributes %d - %d: %s".format(path(i), path((i+1)%path.length), commonAttr.mkString(", ")))
        //        println("i: %d, join size: %f".format(i, joinSize))

        for (attr <- commonAttr) {
          val frequency1 = intermedFreqs(attr)
          val frequency2 = getMasterFrequency(attr)

          val attrDomain = frequency1.keys.toList.union(frequency2.keys.toList)

          val joinFrequency = (for (attrValue <- attrDomain) yield {
            val count1 = if (frequency1.contains(attrValue)) frequency1(attrValue) else 0
            val count2 = if (frequency2.contains(attrValue)) frequency2(attrValue) else 0
            (attrValue, (((count1 * count2) / denoms(attr)) * joinSize))
          }).toMap

          intermedFreqs += (attr -> joinFrequency)
        }

        val leftAttr = attrSoFar.diff(commonAttr)

        for (attr <- leftAttr) {
          val frequency = intermedFreqs(attr)
          val attrDomain = frequency.keys
          val joinFrequency = (for (attrValue <- attrDomain) yield {
            (attrValue, (frequency(attrValue) * joinSize) / joinSizeSoFar)
          }).toMap
          intermedFreqs += (attr -> joinFrequency)
        }

        val rightAttr = dsAttrs(path((i + 1) % path.length)).diff(commonAttr)

        for (attr <- rightAttr) {
          val frequency = getMasterFrequency(attr)
          val attrDomain = frequency.keys
          val joinFrequency = (for (attrValue <- attrDomain) yield {
            (attrValue, (frequency(attrValue) * joinSize) / joinSizeSoFar)
          }).toMap
          intermedFreqs += (attr -> joinFrequency)
        }

        attrSoFar = attrSoFar.union(dsAttrs(path((i + 1) % path.length)))
        joinSizeSoFar = joinSize
      }

      println("Join size of path: %f".format(joinSizeSoFar))

    }
  }

  def entroPath(allPaths: List[Array[Int]]) {
    for (path <- allPaths) {

      var intermedEntropies = Map[String, Double]()
      for (attr <- attrIndex.keys) {
        intermedEntropies += (attr -> getMasterEntropy(attr))
      }

      val sumTerms = for (i <- 1 to path.length - 2) yield {
        val commonAttr = dsAttrs(path(i)).intersect(dsAttrs(path(i + 1)))
        val entropies2 = for (attr <- commonAttr) yield {
          getMasterEntropy(attr)
        }
        val entropies1 = if (i == 0) Array.fill[Double](commonAttr.length)(0.0).toList else entropies2
        val entropies = for (i <- 0 to commonAttr.length - 1) yield {
          Math.max(entropies1(i), entropies2(i))
        }
        entropies.sum
      }

      val sum = sumTerms.sum

      println("sum: " + sum)
    }
  }

  def test(sc: SparkContext, option: String) {
    val testSamples = Random.shuffle(master.toList).take(500)
    //     getAccuracyNormalJoin(sc, testSamples, option)
    getAccuracyProbabilisticJoin(sc, testSamples, option)
  }

  def getAccuracyNormalJoin(sc: SparkContext, testSamples: List[Array[String]], option: String) {

    for (path <- getAllPaths(0, 5)) {

      val successCountK = Array.fill[Int](5)(0)

      for (sample <- testSamples) {

        val origAge = sample(attrIndex("age"))

        var ldb = List(sample)
        var attrSoFar = dsAttrs(path(0))

        for (i <- 0 to path.length - 2) {

          var rdb = if (option == "cor5") cor5(path(i + 1) - 1)
          else if (option == "cor10") cor10(path(i + 1) - 1)
          else master
          val commonAttr = attrSoFar.intersect(dsAttrs(path(i + 1)))

          var jdb = ListBuffer[Array[String]]()
          for (lTuple <- ldb) {
            for (rTuple <- rdb) {
              if (equal(lTuple, rTuple, commonAttr, option)) {
                jdb += rTuple
              }
            }
          }
          attrSoFar = attrSoFar.union(dsAttrs(path(i + 1)))
          ldb = jdb.toList
        }

        val ages = ldb.map { tuple =>
          tuple(attrIndex("age"))
        }.groupBy(identity).mapValues { _.size }.toSeq.sortBy(x => x._2 > x._2).map(_._1)

        for {
          k <- 0 to successCountK.length - 1
          if (k < ages.length)
        } {
          if (ages(k) == origAge) {
            for (i <- k to successCountK.length - 1) {
              successCountK(i) += 1
            }
          }
        }
        //        println("final size " + ldb.length)
      }

      println("path: %s".format(path.mkString(" - ")))
      println("accuracy: %s".format(successCountK.map(x => (x * 100) / testSamples.length).mkString(",")))

    }

  }

  def getAccuracyProbabilisticJoin(sc: SparkContext, testSamples: List[Array[String]], option: String) {

    for (path <- getAllPaths(0, 5)) {
      println("path: %s".format(path.mkString(" - ")))
      for (k <- 1 to 5) {

        val successCountK = Array.fill[Int](5)(0)
        var success = 0
        var lengthSum = 0
        for (sample <- testSamples) {

          val origAge = sample(attrIndex("age"))
          var ldb = List((sample, 1.0))

          for (i <- 0 to path.length - 2) {
            val rdb = if (option == "cor5") cor5(path(i + 1) - 1)
            else if (option == "cor10") cor10(path(i + 1) - 1)
            else master
            val commonAttr = dsAttrs(path(i)).intersect(dsAttrs(path(i + 1)))

            val ldbTotalScore = ldb.map(_._2).sum

            val distinctKeyValueScores = ldb.map { ts =>
              val tuple = ts._1
              val score = ts._2
              val keyValue = tupleToCommonAttrMap(tuple, commonAttr)
              (keyValue, score)
            }
              .groupBy(_._1).mapValues(_.map(_._2).sum / ldbTotalScore)
              .toSeq.sortBy(x => x._2 > x._2).take(k)

            var jdb = ListBuffer[(Array[String], Double)]()
            for (lKeyValueScore <- distinctKeyValueScores) {
              for (rTuple <- rdb) {
                val lKeyValue = lKeyValueScore._1
                val lScore = lKeyValueScore._2
                val rKeyValue = tupleToCommonAttrMap(rTuple, commonAttr)
                if (probEqual(lKeyValue, rKeyValue, option)) {
                  jdb += ((rTuple, lScore))
                }
              }
            }
            ldb = jdb.toList
          }

          val agesAll = ldb.map { tupleScore =>
            val tuple = tupleScore._1
            val score = tupleScore._2
            (tuple(attrIndex("age")), score)
          }
            .groupBy(_._1).mapValues(_.map(_._2).sum)
            .toSeq.sortBy(x => x._2 > x._2).map(_._1)

          lengthSum += agesAll.length

          val ages = agesAll.take(k)

          if (ages.contains(origAge)) success += 1

        }
        println("length: " + lengthSum.toDouble / testSamples.length)
        println("accuracy for k = %d: %d".format(k, (success * 100) / testSamples.length))
      }
    }
  }

  def probEqual(lKeyValue: Map[String, String], rKeyValue: Map[String, String], option: String): Boolean = {
    var probEqual = true
    for (lKey <- lKeyValue.keys) {
      if (option != "master") {
        if (lKey == "first_name" || lKey == "last_name") {
          if (LevenshteinMetric.compare(lKeyValue(lKey), rKeyValue(lKey)).get > LEVENSHTEIN_DIST_THRES) probEqual = false
        } else {
          if (lKeyValue(lKey) != rKeyValue(lKey)) probEqual = false
        }
      } else {
        if (lKeyValue(lKey) != rKeyValue(lKey)) probEqual = false
      }
    }
    probEqual
  }

  def equal(lTuple: Array[String], rTuple: Array[String], commonAttr: List[String], option: String): Boolean = {
    var equal = true
    for {
      attr <- commonAttr
      if (equal == true)
    } {
      if (option != "master") {
        if (attr == "first_name" || attr == "last_name") {
          //          println(lTuple.mkString(",") + " " + rTuple.mkString(","))
          if (LevenshteinMetric.compare(lTuple(attrIndex(attr)), rTuple(attrIndex(attr))).get > LEVENSHTEIN_DIST_THRES) equal = false
        } else {
          if (lTuple(attrIndex(attr)) != rTuple(attrIndex(attr))) equal = false
        }
      } else {
        if (lTuple(attrIndex(attr)) != rTuple(attrIndex(attr))) equal = false
      }
    }
    equal
  }

  def tupleToCommonAttrMap(tuple: Array[String], commonAttr: List[String]): Map[String, String] = {
    var keyValue = Map[String, String]()
    for (attr <- commonAttr) {
      keyValue += (attr -> tuple(attrIndex(attr)))
    }
    keyValue
  }

  /* ------ New code starts here ------- */

  def extractNGrams(str: String, n: Int): Seq[String] = {
    for (i <- 0 to str.length() - n) yield str.substring(i, i + n)
  }

  def blockJoin(sc: SparkContext, pw: PrintWriter, dsSize: Int, path: Array[Int], corrLevel: Int, levensteinThres: Int) {

    val startTime = System.currentTimeMillis()

    val spark = new org.apache.spark.sql.SQLContext(sc)
    import spark.implicits._

    val datasets = Array.ofDim[RDD[Array[String]]](NUM_DATASETS)

    val ORIG_DS_PATH = DIST_DATASET_DIR + "NC16_%d_0.csv".format(dsSize)
    val testSamples = sc.textFile(ORIG_DS_PATH).takeSample(false, NUM_TEST_SAMPLES, 121690).map(_.split(COMMA, -1))

    for (i <- 1 to NUM_DATASETS - 1) {
      var distDsPath = ""
      if (corrLevel == 0) distDsPath = DIST_DATASET_DIR + "NC16_%d_%d.csv".format(dsSize, corrLevel)
      else distDsPath = DIST_DATASET_DIR + "NC16_%d_%d_%d.csv".format(dsSize, corrLevel, i)

      //      println(distDsPath)
      datasets(i) = sc.textFile(distDsPath).map(_.split(COMMA, -1)).cache()
    }

    var ldb = sc.parallelize(testSamples).zipWithIndex().cache()
    var groundTruth = ldb.map(x => (x._2, x._1(attrIndex(TARGET_ATTRIBUTE)))).collectAsMap()
    var attrSoFar = dsAttrs(path(0))

    for (i <- 0 to path.length - 2) {

      println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))
      pw.println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))

      var rdb = datasets(path(i + 1)).zipWithIndex().cache()

      val commonAttrs = attrSoFar.intersect(dsAttrs(path(i + 1)))

      val commonAttrIndices = commonAttrs.map(attrIndex(_))

      val nGramStartTime = System.currentTimeMillis()

      val nGrams = ldb.union(rdb).flatMap { x =>
        extractNGrams(commonAttrIndices.map(x._1(_)).reduce(_ + _), GRAM_SIZE)
      }.distinct().zipWithIndex().collectAsMap()

      val nGramEndTime = System.currentTimeMillis()
      println(" -- -- ngram map construction time: " + (nGramEndTime - nGramStartTime))

      val ldbRdd = ldb.map { x =>
        val commAttrVal = commonAttrIndices.map(x._1(_)).reduce(_ + _)
        val sparseVecSeq = extractNGrams(commAttrVal, GRAM_SIZE).distinct.map(x => (nGrams(x).toInt, 1.0)).sortBy(_._1).toSeq
        (x._2, Vectors.sparse(nGrams.size, sparseVecSeq), commAttrVal, x._1)
      }

      val rdbRdd = rdb.map { x =>
        val commAttrVal = commonAttrIndices.map(x._1(_)).reduce(_ + _)
        val sparseVecSeq = extractNGrams(commAttrVal, GRAM_SIZE).distinct.map(x => (nGrams(x).toInt, 1.0)).sortBy(_._1).toSeq
        (x._2, Vectors.sparse(nGrams.size, sparseVecSeq), commAttrVal, x._1)
      }

      val lDF = spark.createDataFrame(ldbRdd).toDF("id", "keys", "common", "tuple").cache()
      val rDF = spark.createDataFrame(rdbRdd).toDF("id", "keys", "common", "tuple").cache()

      lDF.count()
      rDF.count()

      val verboseEndTime = System.currentTimeMillis()
      println(" -- -- verbose time: " + (verboseEndTime - nGramEndTime))

      val mh = new MinHashLSH()
        .setNumHashTables(NUM_HASH_TABLES)
        .setInputCol("keys")
        .setOutputCol("values")

      val model = mh.fit(lDF)

      var levensteinThreshold = if (corrLevel == 0) 0 else 1 //Math.ceil(commonAttrs.size * levensteinThresPerAttr)
      levensteinThreshold = levensteinThres

      val joinedDF = model.approxSimilarityJoin(lDF, rDF, DISTANCE_THRESHOLD)
        .select("datasetA.id", "datasetA.common", "datasetA.tuple", "datasetB.id", "datasetB.common", "datasetB.tuple").cache()
        .filter { r =>
          //          println(r(1).asInstanceOf[String] + " : " + r(4).asInstanceOf[String] + " : " + LevenshteinMetric.compare(r(1).asInstanceOf[String], r(4).asInstanceOf[String]).get)
          LevenshteinMetric.compare(r(1).asInstanceOf[String], r(4).asInstanceOf[String]).get <= levensteinThreshold
        }.toDF("lid", "lcommon", "ltuple", "rid", "rcommon", "rtuple").cache()

      println(" -- -- join size: " + joinedDF.count())
      pw.println(" -- -- join size: " + joinedDF.count())

      val joinEndTime = System.currentTimeMillis()
      println("join time: " + (joinEndTime - verboseEndTime))

      ldb = joinedDF.select("lid", "ltuple", "rtuple")
        .map { r =>
          val lid = r(0).asInstanceOf[Long]
          val ltuple = r(1).asInstanceOf[WrappedArray[String]].toArray
          val rtuple = r(2).asInstanceOf[WrappedArray[String]].toArray

          attrSoFar.map(attrIndex(_)).map(i => rtuple(i) = ltuple(i))
          (rtuple, lid)
        }
        .rdd.cache()

      val rddEndTime = System.currentTimeMillis()
      println(" -- -- dataframe to rdd conversion time: " + TimeUnit.MILLISECONDS.toMinutes(rddEndTime - joinEndTime))

      attrSoFar = attrSoFar.union(dsAttrs(path(i + 1)))

    }

    val result = ldb.map { x =>
      (x._2, x._1(attrIndex(TARGET_ATTRIBUTE)))
    }
      .groupByKey()
      .map { x =>
        (x._1, x._2.groupBy(identity).toSeq.sortWith((x, y) => x._2.size > y._2.size).take(5).map(_._1))
      }
      .collectAsMap()

    //    val resultSize = result.map(_._2.size).reduce(_ + _)

    val success = groundTruth.map { x =>
      val hitCount = Array.fill[Int](5)(0)
      if (result.contains(x._1) && result(x._1).toList.contains(x._2)) {
        for (i <- hitCount.size - 1 to result(x._1).toList.indexOf(x._2) by -1) {
          hitCount(i) = 1
        }
      }
      hitCount

    }
      .reduce { (x, y) =>
        val sumSeq = for (i <- 0 to (5 - 1)) yield x(i) + y(i)
        sumSeq.toArray
      }

    println(" -- accuracy: " + success.map(x => "%.2f".format((x * 100.0) / testSamples.size)).mkString(COMMA))
    pw.println(" -- accuracy: " + success.map(x => "%.2f".format((x * 100.0) / testSamples.size)).mkString(COMMA))

    val endTime = System.currentTimeMillis()

    println(" -- total time: %d min".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime)))
    pw.println(" -- total time: %d min".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime)))

  }

  def probabilisticJoin(sc: SparkContext, pw: PrintWriter, dsSize: Int, path: Array[Int], corrLevel: Int, levensteinThres: Int) {

    val startTime = System.currentTimeMillis()

    val datasets = Array.ofDim[RDD[Array[String]]](NUM_DATASETS)

    val ORIG_DS_PATH = DIST_DATASET_DIR + "NC16_%d_0.csv".format(dsSize)
    val testSamples = sc.textFile(ORIG_DS_PATH).takeSample(false, NUM_TEST_SAMPLES, 121690).map(_.split(COMMA, -1))

    for (i <- 1 to NUM_DATASETS - 1) {
      var distDsPath = ""
      if (corrLevel == 0) distDsPath = DIST_DATASET_DIR + "NC16_%d_%d.csv".format(dsSize, corrLevel)
      else distDsPath = DIST_DATASET_DIR + "NC16_%d_%d_%d.csv".format(dsSize, corrLevel, i)

      datasets(i) = sc.textFile(distDsPath).map(_.split(COMMA, -1)).cache()
    }

    val accuracy = Array.fill[Double](5)(0)

    for (k <- 1 to 5) {

      val subStartTime = System.currentTimeMillis()

      var success = 0

      var ldb = sc.parallelize(testSamples).map(x => (x, 1.0)).zipWithIndex().cache()
      var groundTruth = ldb.map(x => (x._2, x._1._1(attrIndex(TARGET_ATTRIBUTE)))).collectAsMap()

      var attrSoFar = dsAttrs(path(0))

      for (i <- 0 to path.length - 2) {

        println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))
        pw.println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))

        val commonAttrs = attrSoFar.intersect(dsAttrs(path(i + 1)))

        val verboseStartTime = System.currentTimeMillis()

        val ldbRdd = ldb.map { x =>
          (x._2, tupleToCommonAttrMap(x._1._1, commonAttrs), x._1._2, x._1._1)
        }.cache()

        val rdb = datasets(path(i + 1)).map(x => (x, 1.0)).zipWithIndex()

        val rdbRdd = rdb.map { x =>
          (x._2, tupleToCommonAttrMap(x._1._1, commonAttrs), x._1._2, x._1._1)
        }.cache()

        ldbRdd.count
        rdbRdd.count

        val verboseEndTime = System.currentTimeMillis()
        println(" -- -- verbose time: " + (verboseEndTime - verboseStartTime))

        val lTotScore = ldbRdd.map(_._3).sum
        val lidTotscore = ldbRdd.map(x => (x._1, x._3)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap()

        val lidTopmaps = ldbRdd.map(x => ((x._1, x._2), x._3))
          .groupByKey()
          .map {
            x => (x._1._1, (x._1._2, x._2.sum / lidTotscore(x._1._1)))
          }
          .groupByKey()
          .flatMap { x =>
            for (mapScore <- x._2.toSeq.sortBy(x => x._2 > x._2).take(k)) yield ((x._1, mapScore._1), mapScore._2)
          }
          .collectAsMap()

        val filteredLdbRdd = ldbRdd.filter(x => lidTopmaps.contains((x._1, x._2))).map(x => (x._1, x._2, lidTopmaps((x._1, x._2)), x._4)).collect

        val filterEndTime = System.currentTimeMillis()
        println(" -- -- filter time: " + (filterEndTime - verboseEndTime))

        ldb = rdbRdd.flatMap { rTuple =>
          for {
            lTuple <- filteredLdbRdd
            if (approxEqual(lTuple._2, rTuple._2, levensteinThres))
          } yield {
            attrSoFar.map(attrIndex(_)).map(i => rTuple._4(i) = lTuple._4(i))
            ((rTuple._4, lTuple._3), lTuple._1)
          }
        }.cache()

        println(" -- -- join size: " + ldb.count())
        pw.println(" -- -- join size: " + ldb.count())

        val joinEndTime = System.currentTimeMillis()
        println(" -- -- join time: " + (joinEndTime - filterEndTime))

        attrSoFar = attrSoFar.union(dsAttrs(path(i + 1)))

      }

      val result = ldb.map { x =>
        ((x._2, x._1._1(attrIndex(TARGET_ATTRIBUTE))), x._1._2)
      }
        .groupByKey()
        .map(x => (x._1._1, (x._1._2, x._2.sum)))
        .groupByKey()
        .map { x =>
          (x._1, x._2.toSeq.sortBy(x => x._2 > x._2).take(k).map(_._1))
        }
        .collectAsMap()

      groundTruth.map { x =>
        if (result.contains(x._1) && result(x._1).toList.contains(x._2)) {
          success += 1
        }
      }

      accuracy(k - 1) = (success * 100.0) / testSamples.length
      println(" -- -- accuracy: " + accuracy(k - 1))
    }
    println(" -- accuracy: %s".format(accuracy.map("%.2f".format(_)).mkString(",")))
    pw.println(" -- accuracy: %s".format(accuracy.map("%.2f".format(_)).mkString(",")))

    val endTime = System.currentTimeMillis()

    println(" -- total time: %d min".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime)))
    pw.println(" -- total time: %d min".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime)))
  }

  def normalJoin(sc: SparkContext, pw: PrintWriter, dsSize: Int, path: Array[Int], corrLevel: Int, levensteinThres: Int) {

    val startTime = System.currentTimeMillis()

    val datasets = Array.ofDim[RDD[Array[String]]](NUM_DATASETS)

    val ORIG_DS_PATH = DIST_DATASET_DIR + "NC16_%d_0.csv".format(dsSize)
    val testSamples = sc.textFile(ORIG_DS_PATH).takeSample(false, NUM_TEST_SAMPLES, 121690).map(_.split(COMMA, -1))

    for (i <- 1 to NUM_DATASETS - 1) {
      var distDsPath = ""
      if (corrLevel == 0) distDsPath = DIST_DATASET_DIR + "NC16_%d_%d.csv".format(dsSize, corrLevel)
      else distDsPath = DIST_DATASET_DIR + "NC16_%d_%d_%d.csv".format(dsSize, corrLevel, i)

      datasets(i) = sc.textFile(distDsPath).map(_.split(COMMA, -1)).cache()
    }

    val accuracy = Array.fill[Double](5)(0)

    val subStartTime = System.currentTimeMillis()

    var ldb = sc.parallelize(testSamples).zipWithIndex().cache()
    var groundTruth = ldb.map(x => (x._2, x._1(attrIndex(TARGET_ATTRIBUTE)))).collectAsMap()

    var attrSoFar = dsAttrs(path(0))

    for (i <- 0 to path.length - 2) {

      println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))
      pw.println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))

      val commonAttrs = attrSoFar.intersect(dsAttrs(path(i + 1)))

      val verboseStartTime = System.currentTimeMillis()

      val ldbRdd = ldb.map { x =>
        (x._2, tupleToCommonAttrMap(x._1, commonAttrs), x._1)
      }.cache()

      val rdb = datasets(path(i + 1)).zipWithIndex()

      val rdbRdd = rdb.map { x =>
        (x._2, tupleToCommonAttrMap(x._1, commonAttrs), x._1)
      }.cache()

      ldbRdd.count
      rdbRdd.count

      val verboseEndTime = System.currentTimeMillis()
      println(" -- -- verbose time: " + (verboseEndTime - verboseStartTime))

      val collectedLdbRdd = ldbRdd.collect

      ldb = rdbRdd.flatMap { rTuple =>
        for {
          lTuple <- collectedLdbRdd
          if (approxEqual(lTuple._2, rTuple._2, levensteinThres))
        } yield {
          attrSoFar.map(attrIndex(_)).map(i => rTuple._3(i) = lTuple._3(i))
          (rTuple._3, lTuple._1)
        }
      }.cache()

      println(" -- -- join size: " + ldb.count())
      pw.println(" -- -- join size: " + ldb.count())

      val joinEndTime = System.currentTimeMillis()
      println(" -- -- join time: " + (joinEndTime - verboseEndTime))

      attrSoFar = attrSoFar.union(dsAttrs(path(i + 1)))

    }

    val result = ldb.map { x =>
      (x._2, x._1(attrIndex(TARGET_ATTRIBUTE)))
    }
      .groupByKey()
      .map { x =>
        (x._1, x._2.groupBy(identity).toSeq.sortWith((x, y) => x._2.size > y._2.size).take(5).map(_._1))
      }
      .collectAsMap()

    //    val resultSize = result.map(_._2.size).reduce(_ + _)

    val success = groundTruth.map { x =>
      val hitCount = Array.fill[Int](5)(0)
      if (result.contains(x._1) && result(x._1).toList.contains(x._2)) {
        for (i <- hitCount.size - 1 to result(x._1).toList.indexOf(x._2) by -1) {
          hitCount(i) = 1
        }
      }
      hitCount
    }
    .reduce { (x, y) =>
      val sumSeq = for (i <- 0 to (5 - 1)) yield x(i) + y(i)
      sumSeq.toArray
    }

    println(" -- accuracy: " + success.map(x => "%.2f".format((x * 100.0) / testSamples.size)).mkString(COMMA))
    pw.println(" -- accuracy: " + success.map(x => "%.2f".format((x * 100.0) / testSamples.size)).mkString(COMMA))

    val endTime = System.currentTimeMillis()

    println(" -- total time: %d min".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime)))
    pw.println(" -- total time: %d min".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime)))
  }

  def approxEqual(lCommonAttrMap: Map[String, String], rCommonAttrMap: Map[String, String], levensteinThres: Int): Boolean = {
    val startTime = System.currentTimeMillis()

    val sortedKeys = lCommonAttrMap.keys.toSeq.sortWith(_ < _)
    val lValue = sortedKeys.map(lCommonAttrMap(_)).mkString("")
    val rValue = sortedKeys.map(rCommonAttrMap(_)).mkString("")
    if (LevenshteinMetric.compare(lValue, rValue).get <= levensteinThres) return true
    else return false
  }

}



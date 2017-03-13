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

object Joiner {

  val CASE_NORMAL_JOIN = 0
  val CASE_BLOCK_JOIN = 1
  val CASE_PROBABILISTIC_JOIN = 2
  val CASE_PROBABILISTIC_JOIN_2 = 3
  val CASE_PROBABILISTIC_JOIN_3 = 4
  val CASE_BLOCK_PROBABILISTIC_JOIN_2 = 5

  val SCENE_NC = 0
  val SCENE_FL = 1

  val NUM_TEST_SAMPLES = 500
  val NUM_DATASETS = 6
  val GRAM_SIZE = 3
  val NUM_HASH_TABLES = 5
  val DISTANCE_THRESHOLD = 0.3

  val COMMA = ","

  var dsAttrs: Array[Set[String]] = null
  var attrIndex: Map[String, Int] = null
  var targetAttribute: List[String] = null
  var origDsPathFormat: String = null
  var distDsPathFormat: String = null
  var allPaths: List[Array[Int]] = null
  var datasets: Array[RDD[Array[String]]] = null
  var testSamples: Array[Array[String]] = null

  var normalJoinResultPath: String = null
  var blockJoinResultPath: String = null
  var probabilisticJoinResultPath: String = null
  var probabilisticJoin2ResultPath: String = null
  var probabilisticJoin3ResultPath: String = null
  var blockProbabilisticJoin2ResultPath: String = null

  var pw: PrintWriter = null

  val scene = SCENE_FL

  def main(args: Array[String]): Unit = {

    if (scene == SCENE_NC) {
      dsAttrs = NC_Extractor.dsAttrs
      attrIndex = NC_Extractor.attrIndex
      targetAttribute = NC_Extractor.TARGET_ATTRIBUTES
      origDsPathFormat = NC_Extractor.ORIG_DATASET_PATH_FORMAT
      distDsPathFormat = NC_Extractor.DIST_DATASET_PATH_FORMAT
      allPaths = NC_Extractor.getAllPaths()
      normalJoinResultPath = NC_Extractor.NORMALJOIN_RESULT_PATH
      blockJoinResultPath = NC_Extractor.BLOCKJOIN_RESULT_PATH
      probabilisticJoinResultPath = NC_Extractor.PROBABILISTICJOIN_RESULT_PATH
      probabilisticJoin2ResultPath = NC_Extractor.PROBABILISTICJOIN_2_RESULT_PATH
      probabilisticJoin3ResultPath = NC_Extractor.PROBABILISTICJOIN_3_RESULT_PATH
      blockProbabilisticJoin2ResultPath = NC_Extractor.BLOCK_PROBABILISTICJOIN_2_RESULT_PATH

    } else if (scene == SCENE_FL) {
      dsAttrs = FL_Extractor.dsAttrs
      attrIndex = FL_Extractor.attrIndex
      targetAttribute = FL_Extractor.TARGET_ATTRIBUTES
      origDsPathFormat = FL_Extractor.ORIG_DATASET_PATH_FORMAT
      distDsPathFormat = FL_Extractor.DIST_DATASET_PATH_FORMAT
      allPaths = FL_Extractor.getAllPaths()
      normalJoinResultPath = FL_Extractor.NORMALJOIN_RESULT_PATH
      blockJoinResultPath = FL_Extractor.BLOCKJOIN_RESULT_PATH
      probabilisticJoinResultPath = FL_Extractor.PROBABILISTICJOIN_RESULT_PATH
      probabilisticJoin2ResultPath = FL_Extractor.PROBABILISTICJOIN_2_RESULT_PATH
      probabilisticJoin3ResultPath = FL_Extractor.PROBABILISTICJOIN_3_RESULT_PATH
      blockProbabilisticJoin2ResultPath = FL_Extractor.BLOCK_PROBABILISTICJOIN_2_RESULT_PATH
    }

    for (dsSize <- List(100000)) {
      for (option <- List(CASE_PROBABILISTIC_JOIN_3)) {
        for (path <- allPaths){
          for (corrLevel <- List(0, 5, 10)) {
            for (levensteinThres <- List(0)) {

              val sc = Spark.getContext()

              datasets = getDatasets(sc, dsSize, corrLevel)
              testSamples = getTestSamples(sc, dsSize)

              if (option == CASE_NORMAL_JOIN) {
                pw = new PrintWriter(new FileWriter(normalJoinResultPath, true))
                println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))
                pw.println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))

                normalJoin(sc, datasets, testSamples, path, levensteinThres)

                pw.flush
                pw.close
              } else if (option == CASE_PROBABILISTIC_JOIN) {
                pw = new PrintWriter(new FileWriter(probabilisticJoinResultPath, true))
                println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))
                pw.println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))

                probabilisticJoin(sc, datasets, testSamples, path, levensteinThres)

                pw.flush
                pw.close
              } else if (option == CASE_BLOCK_JOIN) {
                pw = new PrintWriter(new FileWriter(blockJoinResultPath, true))
                println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))
                pw.println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))

                blockJoin(sc, datasets, testSamples, path, levensteinThres)

                pw.flush
                pw.close
              } else if (option == CASE_PROBABILISTIC_JOIN_2) {
                pw = new PrintWriter(new FileWriter(probabilisticJoin2ResultPath, true))
                println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))
                pw.println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))

                probabilisticJoin2(sc, datasets, testSamples, path, levensteinThres)

                pw.flush
                pw.close
              } else if (option == CASE_PROBABILISTIC_JOIN_3) {
                pw = new PrintWriter(new FileWriter(probabilisticJoin3ResultPath, true))
                println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))
                pw.println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))

                probabilisticJoin3(sc, datasets, testSamples, path, levensteinThres)

                pw.flush
                pw.close
              } else if (option == CASE_BLOCK_PROBABILISTIC_JOIN_2) {
                pw = new PrintWriter(new FileWriter(blockProbabilisticJoin2ResultPath, true))
                println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))
                pw.println("\n\n\nsize: %d, path: %s, corruption: %d, levenstein: %d".format(dsSize, path.mkString("-"), corrLevel, levensteinThres))

                blockProbabilisticJoin2(sc, datasets, testSamples, path, levensteinThres)

                pw.flush
                pw.close
              }
              sc.stop()
            }
          }
        }
      }
    }
  }

  def getDatasets(sc: SparkContext, dsSize: Int, corrLevel: Int): Array[RDD[Array[String]]] = {
    val numDatasets = dsAttrs.size
    val datasets = Array.ofDim[RDD[Array[String]]](numDatasets)
    var distDsPath = ""
    for (i <- 1 to numDatasets - 1) {
      if (corrLevel == 0) distDsPath = origDsPathFormat.format(dsSize)
      else distDsPath = distDsPathFormat.format(dsSize, corrLevel, i)
      datasets(i) = sc.textFile(distDsPath).map(_.split(COMMA, -1)).cache()
    }

    return datasets
  }

  def getTestSamples(sc: SparkContext, dsSize: Int): Array[Array[String]] = {
    val testSamples = sc.textFile(origDsPathFormat.format(dsSize)).takeSample(false, NUM_TEST_SAMPLES, 121690).map(_.split(COMMA, -1))
    return testSamples
  }

  def extractNGrams(str: String, n: Int): Seq[String] = {
    for (i <- 0 to str.length() - n) yield str.substring(i, i + n)
  }

  def getTargetValue(tuple: Array[String]): String = {
    targetAttribute.map(attrIndex(_)).map(tuple(_)).mkString(COMMA)
  }

  def blockJoin(sc: SparkContext, datasets: Array[RDD[Array[String]]], testSamples: Array[Array[String]], path: Array[Int], levensteinThres: Int) {

    val startTime = System.currentTimeMillis()

    val spark = new org.apache.spark.sql.SQLContext(sc)
    import spark.implicits._

    var ldb = sc.parallelize(testSamples).zipWithIndex().cache()
    var groundTruth = ldb.map(x => (x._2, getTargetValue(x._1))).collectAsMap()
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
        val sparseVecSeq = extractNGrams(commAttrVal, GRAM_SIZE).distinct.map(x => (nGrams(x).toInt, 1.0)).sortWith(_._1 < _._1).toSeq
        (x._2, Vectors.sparse(nGrams.size, sparseVecSeq), commAttrVal, x._1)
      }

      val rdbRdd = rdb.map { x =>
        val commAttrVal = commonAttrIndices.map(x._1(_)).reduce(_ + _)
        val sparseVecSeq = extractNGrams(commAttrVal, GRAM_SIZE).distinct.map(x => (nGrams(x).toInt, 1.0)).sortWith(_._1 < _._1).toSeq
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

      val joinedDF = model.approxSimilarityJoin(lDF, rDF, DISTANCE_THRESHOLD)
        .select("datasetA.id", "datasetA.common", "datasetA.tuple", "datasetB.id", "datasetB.common", "datasetB.tuple").cache()
        .filter { r =>
          approxEqual(r(1).asInstanceOf[String], r(4).asInstanceOf[String], levensteinThres)
        }.toDF("lid", "lcommon", "ltuple", "rid", "rcommon", "rtuple").cache()

      println(" -- -- join size: " + joinedDF.count())
      pw.println(" -- -- join size: " + joinedDF.count())

      val joinEndTime = System.currentTimeMillis()
      println(" -- -- join time: " + (joinEndTime - verboseEndTime))

      ldb = joinedDF.select("lid", "ltuple", "rtuple")
        .map { r =>
          val lid = r(0).asInstanceOf[Long]
          val ltuple = r(1).asInstanceOf[WrappedArray[String]].toArray
          val rtuple = r(2).asInstanceOf[WrappedArray[String]].toArray

          attrSoFar.map(attrIndex(_)).map(i => rtuple(i) = ltuple(i))
          (rtuple, lid)
        }
        .rdd.cache()

      ldb.count

      val rddEndTime = System.currentTimeMillis()
      println(" -- -- dataframe to rdd conversion time: " + TimeUnit.MILLISECONDS.toMinutes(rddEndTime - joinEndTime))

      attrSoFar = attrSoFar.union(dsAttrs(path(i + 1)))

    }

    val result = ldb.map { x =>
      (x._2, getTargetValue(x._1))
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

    println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
    pw.println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))

  }

  def probabilisticJoin(sc: SparkContext, datasets: Array[RDD[Array[String]]], testSamples: Array[Array[String]], path: Array[Int], levensteinThres: Int) {

    val startTime = System.currentTimeMillis()

    val accuracy = Array.fill[Double](5)(0)

    for (k <- 5 to 5) {

      val subStartTime = System.currentTimeMillis()

      var success = 0

      var ldb = sc.parallelize(testSamples).map(x => (x, 1.0)).zipWithIndex().cache()
      var groundTruth = ldb.map(x => (x._2, getTargetValue(x._1._1))).collectAsMap()

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
            for (mapScore <- x._2.toSeq.sortWith(_._2 > _._2).take(k)) yield ((x._1, mapScore._1), mapScore._2)
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
        ((x._2, getTargetValue(x._1._1)), x._1._2)
      }
        .groupByKey()
        .map(x => (x._1._1, (x._1._2, x._2.sum)))
        .groupByKey()
        .map { x =>
          (x._1, x._2.toSeq.sortWith(_._2 > _._2).take(k).map(_._1))
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

    println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
    pw.println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
  }

  def probabilisticJoin2(sc: SparkContext, datasets: Array[RDD[Array[String]]], testSamples: Array[Array[String]], path: Array[Int], levensteinThres: Int) {
    for (k <- List(5)) {
    val startTime = System.currentTimeMillis()

    

      val subStartTime = System.currentTimeMillis()

      var ldb = sc.parallelize(testSamples).map(x => (x, 1.0)).zipWithIndex().cache()
      var groundTruth = ldb.map(x => (x._2, getTargetValue(x._1._1))).collectAsMap()

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

        val lidTotscore = ldbRdd.map(x => (x._1, x._3)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap()

        val lidTopmaps = ldbRdd.map(x => ((x._1, x._2), x._3))
          .groupByKey()
          .map { x =>
            (x._1._1, (x._1._2, x._2.sum / lidTotscore(x._1._1)))
          }
          .groupByKey()
          .flatMap { x =>
            for (mapScore <- x._2.toSeq.sortWith(_._2 > _._2).take(k)) yield ((x._1, mapScore._1), mapScore._2)
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
        ((x._2, getTargetValue(x._1._1)), x._1._2)
      }
        .groupByKey()
        .map(x => (x._1._1, (x._1._2, x._2.sum)))
        .groupByKey()
        .map { x =>
          (x._1, x._2.toSeq.sortWith(_._2 > _._2).take(5).map(_._1))
        }
        .collectAsMap()

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

      println(" -- accuracy for k = " + k + ": " + success.map(x => "%.2f".format((x * 100.0) / testSamples.size)).mkString(COMMA))
      pw.println(" -- accuracy for k = " + k + ": " + success.map(x => "%.2f".format((x * 100.0) / testSamples.size)).mkString(COMMA))

    

    val endTime = System.currentTimeMillis()

    println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
    pw.println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
  }
  }
  def probabilisticJoin3(sc: SparkContext, datasets: Array[RDD[Array[String]]], testSamples: Array[Array[String]], path: Array[Int], levensteinThres: Int) {

    for (k <- List(5, 40)) {
      val startTime = System.currentTimeMillis()

      var ldb = sc.parallelize(testSamples).map(x => (x, 1.0)).zipWithIndex().cache()
      var groundTruth = ldb.map(x => (x._2, getTargetValue(x._1._1))).collectAsMap()

      var attrSoFar = dsAttrs(path(0))

      for (i <- 0 to path.length - 2) {

        println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))
        pw.println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))

        val commonAttrs = attrSoFar.intersect(dsAttrs(path(i + 1)))

        val filterStartTime = System.currentTimeMillis()

        val lidTotscore = ldb.map(x => (x._2, x._1._2)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap()

        val lidAttrTopmap = ldb.flatMap { x =>
          attrSoFar.map(attrIndex(_)).map { ai =>
            ((x._2, ai, x._1._1(ai)), x._1._2)
          }
        }
          .groupByKey()
          .map { x =>
            ((x._1._1, x._1._2), (x._1._3, x._2.sum / lidTotscore(x._1._1)))
          }
          .groupByKey()
          .flatMap { x =>
            for (valueScore <- x._2.toSeq.sortWith(_._2 > _._2).take(k)) yield ((x._1._1, x._1._2, valueScore._1), valueScore._2)
          }
          .collectAsMap()


        val filteredLdb = ldb.filter { x =>
          val lid = x._2
          val tuple = x._1._1
          attrSoFar.map(attrIndex(_)).map { ai =>
            lidAttrTopmap.contains(lid, ai, tuple(ai))
          }.reduce(_ & _)
        }
          .map { x =>
            val lid = x._2
            val tuple = x._1._1

            val score = commonAttrs.map(attrIndex(_)).map { ai =>
              lidAttrTopmap(lid, ai, tuple(ai))
            }.reduce(_ + _)

            ((tuple, score / commonAttrs.size), lid)
          }

        filteredLdb.count

        val filterEndTime = System.currentTimeMillis()
        println(" -- -- filter time: " + (filterEndTime - filterStartTime))

        val filteredLdbRdd = filteredLdb.map { x =>
          (x._2, tupleToCommonAttrMap(x._1._1, commonAttrs), x._1._2, x._1._1)
        }.collect()

        val rdb = datasets(path(i + 1)).map(x => (x, 1.0)).zipWithIndex()

        val rdbRdd = rdb.map { x =>
          (x._2, tupleToCommonAttrMap(x._1._1, commonAttrs), x._1._2, x._1._1)
        }.cache()

        rdbRdd.count

        val verboseEndTime = System.currentTimeMillis()
        println(" -- -- verbose time: " + (verboseEndTime - filterEndTime))

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

//      val result = ldb.map { x =>
//        ((x._2, getTargetValue(x._1._1)), x._1._2)
//      }
//        .groupByKey()
//        .map(x => (x._1._1, (x._1._2, x._2.sum)))
//        .groupByKey()
//        .map { x =>
//          (x._1, x._2.toSeq.sortWith(_._2 > _._2).take(5).map(_._1))
//        }
//        .collectAsMap()
        
        
      val result2 = ldb.map{ x =>
        ((x._2, getTargetValue(x._1._1)), 1)
      }
      .groupByKey()
      .map(x => (x._1._1, (x._1._2, x._2.sum)))
      .groupByKey()
      .map { x =>
        (x._1, x._2.toSeq.sortWith(_._2 > _._2).take(5).map(_._1))
      }
      .collectAsMap()
          
        

      val success = groundTruth.map { x =>
        val hitCount = Array.fill[Int](5)(0)
        if (result2.contains(x._1) && result2(x._1).toList.contains(x._2)) {
          for (i <- hitCount.size - 1 to result2(x._1).toList.indexOf(x._2) by -1) {
            hitCount(i) = 1
          }
        }
        hitCount
      }
        .reduce { (x, y) =>
          val sumSeq = for (i <- 0 to (5 - 1)) yield x(i) + y(i)
          sumSeq.toArray
        }

      println(" -- accuracy for k = " + k + ": " + success.map(x => "%.2f".format((x * 100.0) / testSamples.size)).mkString(COMMA))
      pw.println(" -- accuracy for k = " + k + ": " + success.map(x => "%.2f".format((x * 100.0) / testSamples.size)).mkString(COMMA))

      val endTime = System.currentTimeMillis()

      println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
      pw.println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
    }
  }

  def blockProbabilisticJoin2(sc: SparkContext, datasets: Array[RDD[Array[String]]], testSamples: Array[Array[String]], path: Array[Int], levensteinThres: Int) {

    val k = 40

    val startTime = System.currentTimeMillis()

    val spark = new SQLContext(sc)
    import spark.implicits._

    var ldb = sc.parallelize(testSamples).map(x => (x, 1.0)).zipWithIndex().cache()
    var groundTruth = ldb.map(x => (x._2, getTargetValue(x._1._1))).collectAsMap()

    var attrSoFar = dsAttrs(path(0))

    for (i <- 0 to path.length - 2) {

      println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))
      pw.println(" -- joining: D%d & D%d".format(path(i), path(i + 1)))

      var rdb = datasets(path(i + 1)).map(x => (x, 1.0)).zipWithIndex().cache()

      val commonAttrs = attrSoFar.intersect(dsAttrs(path(i + 1)))

      val commonAttrIndices = commonAttrs.map(attrIndex(_))

      val filterStartTime = System.currentTimeMillis()

      val lidTotscore = ldb.map(x => (x._2, x._1._2)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap()

      val lidTopmaps = ldb.map(x => ((x._2, tupleToCommonAttrMap(x._1._1, commonAttrs)), x._1._2))
        .groupByKey()
        .map { x =>
          (x._1._1, (x._1._2, x._2.sum / lidTotscore(x._1._1)))
        }
        .groupByKey()
        .flatMap { x =>
          for (mapScore <- x._2.toSeq.sortWith(_._2 > _._2).take(k)) yield ((x._1, mapScore._1), mapScore._2)
        }
        .collectAsMap()

      val filteredLdb = ldb.filter(x => lidTopmaps.contains((x._2, tupleToCommonAttrMap(x._1._1, commonAttrs))))
        .map { x =>
          ((x._1._1, lidTopmaps(x._2, tupleToCommonAttrMap(x._1._1, commonAttrs))), x._2)
        }

      filteredLdb.count

      val filterEndTime = System.currentTimeMillis()
      println(" -- -- filter time: " + (filterEndTime - filterStartTime))

      val nGrams = filteredLdb.union(rdb).flatMap { x =>
        extractNGrams(commonAttrIndices.map(x._1._1(_)).reduce(_ + _), GRAM_SIZE)
      }.distinct().zipWithIndex().collectAsMap()

      val nGramEndTime = System.currentTimeMillis()
      println(" -- -- ngram map construction time: " + (nGramEndTime - filterEndTime))

      val filteredLdbRdd = filteredLdb.map { x =>
        val commAttrVal = commonAttrIndices.map(x._1._1(_)).reduce(_ + _)
        val sparseVecSeq = extractNGrams(commAttrVal, GRAM_SIZE).distinct.map(x => (nGrams(x).toInt, 1.0)).sortWith(_._1 < _._1).toSeq
        (x._2, Vectors.sparse(nGrams.size, sparseVecSeq), commAttrVal, x._1._1, x._1._2)
      }

      val rdbRdd = rdb.map { x =>
        val commAttrVal = commonAttrIndices.map(x._1._1(_)).reduce(_ + _)
        val sparseVecSeq = extractNGrams(commAttrVal, GRAM_SIZE).distinct.map(x => (nGrams(x).toInt, 1.0)).sortWith(_._1 < _._1).toSeq
        (x._2, Vectors.sparse(nGrams.size, sparseVecSeq), commAttrVal, x._1._1, x._1._2)
      }

      val lDF = spark.createDataFrame(filteredLdbRdd).toDF("id", "keys", "common", "tuple", "score").cache()
      val rDF = spark.createDataFrame(rdbRdd).toDF("id", "keys", "common", "tuple", "score").cache()

      lDF.count()
      rDF.count()

      val verboseEndTime = System.currentTimeMillis()
      println(" -- -- verbose time: " + (verboseEndTime - nGramEndTime))

      val mh = new MinHashLSH()
        .setNumHashTables(NUM_HASH_TABLES)
        .setInputCol("keys")
        .setOutputCol("values")

      val model = mh.fit(lDF)

      val joinedDF = model.approxSimilarityJoin(lDF, rDF, DISTANCE_THRESHOLD)
        .select("datasetA.id", "datasetA.common", "datasetA.tuple", "datasetA.score", "datasetB.id", "datasetB.common", "datasetB.tuple").cache()
        .filter { r =>
          approxEqual(r(1).asInstanceOf[String], r(5).asInstanceOf[String], levensteinThres)
        }.toDF("lid", "lcommon", "ltuple", "lscore", "rid", "rcommon", "rtuple").cache()

      println(" -- -- join size: " + joinedDF.count())
      pw.println(" -- -- join size: " + joinedDF.count())

      val joinEndTime = System.currentTimeMillis()
      println(" -- -- join time: " + (joinEndTime - verboseEndTime))

      ldb = joinedDF.select("lid", "ltuple", "rtuple", "lscore")
        .map { r =>
          val lid = r(0).asInstanceOf[Long]
          val ltuple = r(1).asInstanceOf[WrappedArray[String]].toArray
          val rtuple = r(2).asInstanceOf[WrappedArray[String]].toArray
          val lscore = r(3).asInstanceOf[Double]

          attrSoFar.map(attrIndex(_)).map(i => rtuple(i) = ltuple(i))
          ((rtuple, lscore), lid)
        }
        .rdd.cache()

      ldb.count

      val rddEndTime = System.currentTimeMillis()
      println(" -- -- dataframe to rdd conversion time: " + TimeUnit.MILLISECONDS.toMinutes(rddEndTime - joinEndTime))

      attrSoFar = attrSoFar.union(dsAttrs(path(i + 1)))

    }

    val result = ldb.map { x =>
      ((x._2, getTargetValue(x._1._1)), x._1._2)
    }
      .groupByKey()
      .map(x => (x._1._1, (x._1._2, x._2.sum)))
      .groupByKey()
      .map { x =>
        (x._1, x._2.toSeq.sortWith(_._2 > _._2).take(5).map(_._1))
      }
      .collectAsMap()

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

    println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
    pw.println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))

  }

  def normalJoin(sc: SparkContext, datasets: Array[RDD[Array[String]]], testSamples: Array[Array[String]], path: Array[Int], levensteinThres: Int) {

    val startTime = System.currentTimeMillis()

    val subStartTime = System.currentTimeMillis()

    var ldb = sc.parallelize(testSamples).zipWithIndex().cache()
    var groundTruth = ldb.map(x => (x._2, getTargetValue(x._1))).collectAsMap()

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
      (x._2, getTargetValue(x._1))
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

    println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
    pw.println(" -- total time: %d min or %d sec".format(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime), TimeUnit.MILLISECONDS.toSeconds(endTime - startTime)))
  }

  def approxEqual(lCommonAttrMap: Map[String, String], rCommonAttrMap: Map[String, String], levensteinThres: Int): Boolean = {

    if (levensteinThres == 0) return lCommonAttrMap == rCommonAttrMap

    val sortedKeys = lCommonAttrMap.keys.toSeq.sortWith(_ < _)
    val lValue = sortedKeys.map(lCommonAttrMap(_)).mkString("")
    val rValue = sortedKeys.map(rCommonAttrMap(_)).mkString("")

    if (LevenshteinMetric.compare(lValue, rValue).get <= levensteinThres) return true
    else return false
  }

  def approxEqual(lCommon: String, rCommon: String, levensteinThres: Int): Boolean = {
    if (levensteinThres == 0) return lCommon == rCommon

    if (LevenshteinMetric.compare(lCommon, rCommon).get <= levensteinThres) return true
    else return false
  }

  def tupleToCommonAttrMap(tuple: Array[String], commonAttrs: Set[String]): Map[String, String] = {
    var keyValue = Map[String, String]()
    for (attr <- commonAttrs) {
      keyValue += (attr -> tuple(attrIndex(attr)))
    }
    keyValue
  }
}
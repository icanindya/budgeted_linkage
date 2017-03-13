package com.icanindya.linkage

import org.apache.spark.SparkContext
import java.io.PrintWriter
import scala.io.Source
import java.io.File

object FL_Extractor {

  val ORIG_FILE_PATH = "E:/Data/Linkage/FL/FL16/data"
  val SAMP_FILE_PATH = "E:/Data/Linkage/FL/FL16/sampled/%d_%d"
  val DIST_DATASET_DIR = "E:/Data/Linkage/FL/FL16/sampled/csv/"
  val CSV_FILE_PATH_FORMAT = DIST_DATASET_DIR + "FL16_%d.csv" //.format(dsSize)
 
  val HISTOGRAM_FILE_PATH_FORMAT = "E:/Data/Linkage/FL/FL16/sampled/histograms/%s.txt" //.format(i)
  val ENTROPY_FILE_PATH = "E:/Data/Linkage/FL/FL16/sampled/entropies/entropy.txt"

  val NORMALJOIN_RESULT_PATH = "E:/Data/Linkage/FL/FL16/result/normal_join_result.txt"
  val BLOCKJOIN_RESULT_PATH = "E:/Data/Linkage/FL/FL16/result/block_join_result.txt"
  val PROBABILISTICJOIN_RESULT_PATH = "E:/Data/Linkage/FL/FL16/result/probabilistic_join_result.txt"
  val PROBABILISTICJOIN_2_RESULT_PATH = "E:/Data/Linkage/FL/FL16/result/probabilistic_join_2_result.txt"
  val PROBABILISTICJOIN_3_RESULT_PATH = "E:/Data/Linkage/FL/FL16/result/probabilistic_join_3_result.txt"
  val BLOCK_PROBABILISTICJOIN_2_RESULT_PATH = "E:/Data/Linkage/FL/FL16/result/block_probabilistic_join_2_result.txt"

  val ORIG_DATASET_PATH_FORMAT = DIST_DATASET_DIR + "FL16_%d_0.csv" //.format(dsSize)
  val DIST_DATASET_PATH_FORMAT = DIST_DATASET_DIR + "FL16_%d_%d_%d.csv" //.format(dsSize, corrLevel, i)

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
  val DISTAFLE_THRESHOLD = 0.3
  val LEVENSTEIN_THRESHOLD = 0
  val TARGET_ATTRIBUTES = List("first_name", "last_name")

  val NUM_CORRUPTED_ATTR = 7
  val NUM_ATTR = 15
  val levensteinThresPerAttr = NUM_CORRUPTED_ATTR / NUM_ATTR.toDouble

  val COUNTY = 0
  val VOTER_NUM = 1
  val LAST_NAME = 2
  val FIRST_NAME = 4
  val MIDDLE_NAME = 5
  val ADDRESS_LINE1 = 7
  val ADDRESS_LINE2 = 8
  val ADDRESS_CITY = 9
  val ADDRESS_STATE = 10
  val ZIP = 11
  val SEX = 19
  val RACE = 20
  val BIRTH_DATE = 21
  val REG_DATE = 22
  val PARTY = 23
  val PHONE_CODE = 34
  val PHONE_NUM = 35
  val EMAIL = 37

  // 0 based column indices for FL 16 attributes
  val attrIndex = Map(
    "first_name" -> 0,
    "middle_name" -> 1,
    "last_name" -> 2,
    "sex" -> 3,
    "race" -> 4,
    "dob" -> 5,
    "zip" -> 6,
    "county" -> 7,
    "party" -> 8,
    "reg_date" -> 9,
    "phone" -> 10,
    "voter_num" -> 11,
    "email" -> 12,
    "address" -> 13)
    
//  MARIA,JANEL,ROTH,F,2,02/27/1972,34983,STL,DEM,03/07/2013,9187,120771452,LEOANDJANEL@MSN.COM,508  SE BROOKSIDE TER PT ST LUCIE
  

  val dummyFrequency = Map(
    "first_name" -> Map[String, Double](),
    "middle_name" -> Map[String, Double](),
    "last_name" -> Map[String, Double](),
    "sex" -> Map("F" -> 1.0),
    "race" -> Map("2" -> 1.0),
    "dob" -> Map[String, Double](),
    "zip" -> Map("34983" -> 1.0),
    "county" -> Map("STL" -> 1.0),
    "party" -> Map[String, Double](),
    "reg_date" -> Map[String, Double](),
    "phone" -> Map("9187" -> 1.0),
    "voter_num" -> Map[String, Double](),
    "email" -> Map[String, Double](),
    "address" -> Map[String, Double]())

  val dsAttrs = Array.ofDim[Set[String]](NUM_DATASETS)
  dsAttrs(0) = Set("phone", "zip", "county", "race", "sex")
  dsAttrs(1) = Set("phone", "county", "voter_num")
  dsAttrs(2) = Set("phone", "race", "zip", "dob")
  dsAttrs(3) = Set("voter_num", "reg_date")
  dsAttrs(4) = Set("zip", "dob", "address")
  dsAttrs(5) = Set("address", "dob", "reg_date", "first_name", "last_name")

  def main(args: Array[String]) {
    val sc = Spark.getContext()
    generateCSV(sc, List((1000, 4), (10000, 4), (100000, 4)))
  }

  def getAllPaths(): List[Array[Int]] = {
    val graph = new Graph(new AdjacencyList(dsAttrs).get())
    graph.allPaths(0, 5)
  }

  def processAndSample(sc: SparkContext, params: List[(Int, Int)]) {
    val lines = sc.textFile(ORIG_FILE_PATH)

    val filtered = lines.map { line =>
      val tokens = line.split(SEPERATOR, -1).map(_.trim().toUpperCase())

      val firstName = tokens(FIRST_NAME)
      val middleName = tokens(MIDDLE_NAME)
      val lastName = tokens(LAST_NAME)

      val sex = tokens(SEX)
      val race = tokens(RACE)
      val dob = tokens(BIRTH_DATE)
      val zip = tokens(ZIP)
      val county = tokens(COUNTY)
      val party = tokens(PARTY)
      val regDate = tokens(REG_DATE)
      val phone = tokens(PHONE_CODE) + tokens(PHONE_NUM)
      val voterNum = tokens(VOTER_NUM)
      val email = tokens(EMAIL)

      val addressLine1 = if (tokens(ADDRESS_LINE1).isEmpty()) "" else tokens(ADDRESS_LINE1) + " "
      val addressLine2 = if (tokens(ADDRESS_LINE2).isEmpty()) "" else tokens(ADDRESS_LINE2) + " "
      val addressCity = if (tokens(ADDRESS_CITY).isEmpty()) "" else tokens(ADDRESS_CITY)

      val address = addressLine1 + addressLine2 + addressCity

      //      println(firstName, middleName, lastName, sex, race, dob, zip, county, party, regDate, phone, voterNum, email, address)
      new FL_Person(firstName, middleName, lastName, sex, race, dob, zip, county, party, regDate, phone, voterNum, email, address)

    }
      .filter { person =>
        !person.firstName.isEmpty() &&
          !person.middleName.isEmpty() &&
          !person.lastName.isEmpty() &&
          !person.sex.isEmpty() &&
          !person.race.isEmpty() &&
          !person.dob.isEmpty() &&
          !person.zip.isEmpty() &&
          !person.county.isEmpty() &&
          !person.party.isEmpty() &&
          !person.regDate.isEmpty() &&
          person.phone.length() == 10 &&
          !person.voterNum.isEmpty() &&
          !person.email.isEmpty() &&
          !person.address.isEmpty() &&
          !person.address.endsWith(" ")
      }
      .map { person =>
        val attributes = Array(person.firstName, person.middleName, person.lastName, person.sex, person.race, person.dob, person.zip, person.county, person.party, person.regDate, person.phone, person.voterNum, person.email, person.address)
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

  // to be used with the ANU corrupter scripts
  def generateCSV(sc: SparkContext, params: List[(Int, Int)]) {

    processAndSample(sc, params)

    for ((sampleSize, phDigits) <- params) {
      val lines = sc.textFile(SAMP_FILE_PATH.format(sampleSize))
      val pw = new PrintWriter(new File(CSV_FILE_PATH_FORMAT.format(sampleSize)))
      pw.println(attrIndex.toSeq.sortBy(_._2).map("\"" + _._1 + "\"").mkString(COMMA))
      for (line <- lines.map(_.split(COMMA, -1).map("\"" + _ + "\"").mkString(COMMA)).collect()) {
        pw.println(line)
      }
      pw.close
    }
  }

  class FL_Person(val firstName: String, val middleName: String, val lastName: String,
                  val sex: String, val race: String, val dob: String,
                  val zip: String, val county: String, val party: String,
                  val regDate: String, val phone: String, val voterNum: String,
                  val email: String, val address: String) {

  }

}
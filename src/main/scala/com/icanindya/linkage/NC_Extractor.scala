package com.icanindya.linkage

import org.apache.spark.SparkContext
import java.io.PrintWriter
import scala.io.Source
import java.io.File

object NC_Extractor {
  // dataset address: https://s3.amazonaws.com/dl.ncsbe.gov/data/Snapshots/VR_Snapshot_20160315.zip

 val ORIG_FILE_PATH = "E:/Data/Linkage/NC/NC16/data"
  val SAMP_FILE_PATH = "E:/Data/Linkage/NC/NC16/sampled/%d_%d"
  val DIST_DATASET_DIR = "E:/Data/Linkage/NC/NC16/sampled/csv/"
  val CSV_FILE_PATH_FORMAT = DIST_DATASET_DIR + "NC16_%d.csv" //.format(dsSize)
 
  val HISTOGRAM_FILE_PATH_FORMAT = "E:/Data/Linkage/NC/NC16/sampled/histograms/%s.txt" //.format(i)
  val ENTROPY_FILE_PATH = "E:/Data/Linkage/NC/NC16/sampled/entropies/entropy.txt"

  val NORMALJOIN_RESULT_PATH = "E:/Data/Linkage/NC/NC16/result/normal_join_result.txt"
  val BLOCKJOIN_RESULT_PATH = "E:/Data/Linkage/NC/NC16/result/block_join_result.txt"
  val PROBABILISTICJOIN_RESULT_PATH = "E:/Data/Linkage/NC/NC16/result/probabilistic_join_result.txt"
  val PROBABILISTICJOIN_2_RESULT_PATH = "E:/Data/Linkage/NC/NC16/result/probabilistic_join_2_result.txt"
  val PROBABILISTICJOIN_3_RESULT_PATH = "E:/Data/Linkage/NC/NC16/result/probabilistic_join_3_result.txt"
  val BLOCK_PROBABILISTICJOIN_2_RESULT_PATH = "E:/Data/Linkage/NC/NC16/result/block_probabilistic_join_2_result.txt"

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
  val TARGET_ATTRIBUTES = List("age")

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

//  LATOYA,NICOLE,BARBER,F,BLACK or AFRICAN AMERICAN,NOT HISPANIC or NOT LATINO,37,NC,28601,CATAWBA,DEM,2008-05-08,3964,000030054611,BN255002,1820 20TH AV DR HICKORY NC  
    
  val dummyFrequency = Map("first_name" -> Map("LATOYA" -> 1.0),
    "middle_name" -> Map("NICOLE" -> 1.0),
    "last_name" -> Map("BARBER" -> 1.0),
    "sex" -> Map("F" -> 1.0),
    "race" -> Map[String, Double](),
    "ethnicity" -> Map[String, Double](),
    "age" -> Map[String, Double](),
    "birth_place" -> Map("NC" -> 1.0),
    "zip" -> Map("28601" -> 1.0),
    "county" -> Map("CATAWBA" -> 1.0),
    "party" -> Map[String, Double](),
    "reg_date" -> Map[String, Double](),
    "phone" -> Map("3964" -> 1.0),
    "voter_num" -> Map[String, Double](),
    "nc_id" -> Map[String, Double](),
    "address" -> Map("1820 20TH AV DR HICKORY NC" -> 1.0))
    

  val dsAttrs = Array.ofDim[Set[String]](NUM_DATASETS)

  dsAttrs(0) = Set("first_name", "middle_name", "last_name", "sex", "address", "zip", "phone", "county", "birth_place")
  dsAttrs(1) = Set("first_name", "middle_name", "reg_date", "zip", "race", "birth_place")
  dsAttrs(2) = Set("last_name", "sex", "address", "voter_num")
  dsAttrs(3) = Set("voter_num", "reg_date", "race", "ethnicity", "party")
  dsAttrs(4) = Set("phone", "county", "nc_id")
  dsAttrs(5) = Set("reg_date", "nc_id", "race", "ethnicity", "party", "age")

  def main(args: Array[String]): Unit = {
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

  class NC_Person(val first_name: String, val middle_name: String, val last_name: String,
                  val sex: String, val race: String, val ethnicity: String, val age: String,
                  val birth_place: String, val zip: String, val county: String,
                  val party: String, val regDate: String, val phone: String,
                  val voterNum: String, val ncId: String, val address: String) extends Serializable {
  }

}



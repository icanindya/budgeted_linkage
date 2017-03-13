package com.icanindya.linkage.migration

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import java.io.PrintWriter
import java.io.FileWriter
import com.icanindya.linkage.migration.Person
import com.icanindya.linkage.Spark
import com.icanindya.linkage.migration.Util
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object MigrationPreprocessor{
  
  var sc = Spark.getContext()
  
  val ncDataFile = "E:/Data/Linkage/NC/NC10/data"
  val ncProcDataFile = "E:/Data/Linkage/NC/NC10/processed"
  val flDataFile = "E:/Data/Linkage/FL/31JUL2016/data"
  val flProcDataFile = "E:/Data/Linkage/FL/31JUL2016/processed"
  
  val ncColSep = "\t"
  val flColSep = "\t"
  
  val NC_HEAD_PREF = "snapshot"
  
  val NC_FN = 12
  val NC_LN = 11
  val NC_MN = 13
  val NC_AGE = 43
  val NC_SEX = 42
  val NC_RACE = 36
  val NC_ETH = 38
  val NC_PARTY = 40
  val NC_AREA_CD = 33
  val NC_PHONE = 34
  
  val FL_FN = 4
  val FL_LN = 2
  val FL_MN = 5
  val FL_DOB = 21
  val FL_SEX = 19
  val FL_RACE = 20
  val FL_BDATE = 21
  val FL_PARTY = 23
  val FL_AREA_CD = 34
  val FL_PHONE = 35
  
  val ncSexMap = Map("MALE" -> "M", "FEMALE" -> "F", "UNK" -> "U")
  var ncRaceMap = 
    Map(
      "WHITE, HISPANIC or LATINO" -> "HS",
      "WHITE, NOT HISPANIC or NOT LATINO" -> "WH",
      "WHITE, UNDESIGNATED" -> "WH",
      
      "ASIAN, HISPANIC or LATINO" -> "HS",
      "ASIAN, NOT HISPANIC or NOT LATINO" -> "AP",
      "ASIAN, UNDESIGNATED" -> "AP",
      
      "AMERICAN INDIAN or ALASKA NATIVE, HISPANIC or LATINO" -> "AI",
      "AMERICAN INDIAN or ALASKA NATIVE, NOT HISPANIC or NOT LATINO" -> "AI",
      "AMERICAN INDIAN or ALASKA NATIVE, UNDESIGNATED" -> "AI",
      
      "BLACK or AFRICAN AMERICAN, HISPANIC or LATINO" -> "BL",
      "BLACK or AFRICAN AMERICAN, NOT HISPANIC or NOT LATINO" -> "BL",
      "BLACK or AFRICAN AMERICAN, UNDESIGNATED" -> "BL",

      "TWO or MORE RACES, HISPANIC or LATINO" -> "HS",
      "TWO or MORE RACES, NOT HISPANIC or NOT LATINO" -> "MR",
      "TWO or MORE RACES, UNDESIGNATED" -> "MR",
      
      "OTHER, HISPANIC or LATINO" -> "HS",
      "OTHER, NOT HISPANIC or NOT LATINO" -> "OT",
      "OTHER, UNDESIGNATED" -> "OT",
      
      "UNDESIGNATED, HISPANIC or LATINO" -> "HS",
      "UNDESIGNATED, NOT HISPANIC or NOT LATINO" -> "UN",
      "UNDESIGNATED, UNDESIGNATED" -> "UN"
    )
  val ncPartyMap = Map("DEMOCRATIC" -> "DEM", "REPUBLICAN" -> "REP", "LIBERTARIAN" -> "LIB", "UNAFFILIATED" -> "UNA", "UNAFFILLIATED" -> "UNA")
  
  val flSexMap = Map("M" -> "M", "F" -> "F", "U" -> "U")
  val flRaceMap = Map("1" -> "AI", "2" -> "AP", "3" -> "BL", "4" -> "HS", "5" -> "WH", "6" -> "OT" , "7" -> "MR", "9" -> "UN")
  val flPartyMap = 
    Map(
      "AIP" -> "UNA",
      "CPF" -> "UNA",
      "DEM" -> "DEM",
      "ECO" -> "UNA",
      "GRE" -> "UNA",
      "IDP" -> "UNA",
      "INT" -> "UNA",
      "LPF" -> "LIB",
      "NPA" -> "UNA",
      "PSL" -> "UNA",
      "REF" -> "UNA",
      "REP" -> "REP"
    )
    
  val DATE = "01/01/2010"
  
  val NA_SX_AG_RC_PR_PHNB = 0
  val NA_SX_AG_RC_PR_PHAB = 1
  val NA_SX_AG_RC_PR = 2
  val NA_SX_AG_RC = 3 
  val NA_SX_AG = 4
  
  val OPTION = NA_SX_AG_RC_PR_PHNB
  
  def main(args: Array[String]): Unit = {
//    extractRequiredData()
//    phoneMatch()
//    println(JaroWinklerMetric.compare("W G Hanley", "Willi Godwin Hanley"))
    joinOnNameSimilarity()
    
    
  }
  
  def extractRequiredData(){
    val ncData = sc.textFile(ncDataFile).filter(!_.startsWith(NC_HEAD_PREF))
    val flData = sc.textFile(flDataFile)
    
    val ncProcData = ncData.map { line =>
      
      val tokens = line.split(ncColSep, -1).map(_.trim()) 
      val fn = tokens(NC_FN).toUpperCase()
      val mn = tokens(NC_MN).toUpperCase()
      val ln = tokens(NC_LN).toUpperCase()
      val sex = ncSexMap(tokens(NC_SEX))
      val age = tokens(NC_AGE)
      val race = ncRaceMap(tokens(NC_RACE) + ", " + tokens(NC_ETH))
      val party = ncPartyMap(tokens(NC_PARTY))
      val phone = tokens(NC_AREA_CD) + tokens(NC_PHONE)
      
      "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(fn, mn, ln, sex, age, race, party, phone)
      
    }
    
    ncProcData.saveAsTextFile(ncProcDataFile)
    
    val flProcData = flData.map { line =>
      
      val tokens = line.split(flColSep, -1).map(_.trim()) 
      val fn = tokens(FL_FN).toUpperCase()
      val mn = tokens(FL_MN).toUpperCase()
      val ln = tokens(FL_LN).toUpperCase()
      val sex = tokens(FL_SEX)
      val age = Util.getAge(tokens(FL_DOB), DATE) 
      val race = flRaceMap(tokens(FL_RACE))
      val party = flPartyMap(tokens(FL_PARTY))
      val phone = tokens(FL_AREA_CD) + tokens(FL_PHONE)
      
      "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(fn, mn, ln, sex, age, race, party, phone)
      
    }
    
    flProcData.saveAsTextFile(flProcDataFile)
    
    println("NC distinct size: %s, FL distinct size: %s".format(ncProcData.count(), flProcData.count()))
  }
  
  def printDomainOfAttr(data: RDD[String]){
    val sexes = data.map(_.split(ncColSep, -1)(NC_SEX).trim()).distinct().collect().mkString(",")
    val races = data.map(_.split(ncColSep, -1)(NC_RACE).trim()).distinct().collect().mkString(",")
    val ethnicities = data.map(_.split(ncColSep, -1)(NC_ETH).trim()).distinct().collect().mkString(",")
    val partyAffiliations = data.map(_.split(ncColSep, -1)(NC_PARTY).trim()).distinct().collect().mkString(",")
    
    println()
    println("Sexes: [%s]".format(sexes))
    println("Races: [%s]".format(races))
    println("Ethnicities: [%s]".format(ethnicities))
    println("Party affiliations: [%s]".format(partyAffiliations))
    
  }
  
  def printRaceEthComination(){
    
    val ncData = sc.textFile(ncDataFile).filter(!_.startsWith(NC_HEAD_PREF))
    
    val a = ncData.map { line => 
      val tokens = line.split(ncColSep, -1).map(_.trim())
      (tokens(NC_RACE), tokens(NC_ETH))
    }.
    countByValue().
    map{ line => 
      ("%s, %s".format(line._1._1, line._1._2), line._2)
    }.toSeq.sortWith(_._1 < _._1).
    map{line =>
      println("%s: %d".format(line._1, line._2))
    }
  }
  
  def phoneMatch(){
    val ncProcData = sc.textFile(ncProcDataFile)
    val flProcData = sc.textFile(flProcDataFile)
    val ncDesData = ncProcData.map { x => x.split("\t", -1).last }.filter { x => x.length() == 10 }
    
    ncDesData.saveAsTextFile("E:/nc")
    
    val flDesData = flProcData.map { x => x.split("\t", -1).last }.filter { x => x.length() == 10}
    
    flDesData.saveAsTextFile("E:/fl")
    
    println("NC: %d, NC_PH: %d, NC_DIST_PH: %d".format(ncProcData.count(), ncDesData.count(), ncDesData.distinct().count()))
    println("FL: %d, FL_PH: %d, FL_DIST_PH: %d".format(flProcData.count(), flDesData.count(), flDesData.distinct().count()))
    
    
//    val ncJoinFl = ncDesData.map((_, null)).join(flDesData.map((_, null))).map(_._1)
//    
//    println("Phone match count: %d".format(ncJoinFl.count()))
  }
  
  val colSep = "\t"
  
  var count = 0L
  
  def joinOnNameSimilarity(){
    
    val pw = new PrintWriter(new FileWriter("E:/Data/Linkage/join_on_name_similarity.txt", true))
    
    val ncProcData = sc.textFile(ncProcDataFile)
    val flProcData = sc.textFile(flProcDataFile)
    
    val ncDesData = getDesiredData(ncProcData).cache()
    val flDesData = getDesiredData(flProcData).cache()
    
    val joined = ncDesData.join(flDesData)
    println("join on race, sex, age, party = %d".format(joined.count()))
    pw.println("join on race, sex, age, party = %d".format(joined.count()))
    pw.flush()
    pw.close()
    
//    var start = System.currentTimeMillis()
//    val desJoin = joined.filter{x => 
//      count += 1
//      if(count%10000 == 0) {
//        var end  = System.currentTimeMillis()
//        println("Jaro time for 10000 = %d".format(end -start))
//        start = System.currentTimeMillis()
//      }
//      
//      val name1 = x._2._1
//      val name2 = x._2._2
//      JaroWinklerMetric.compare(name1, name2).get >= 0.9
//    }
//    
//    println("Join size: %d".format(desJoin.count()))
//    println("Distict join size: %d".format(desJoin.distinct().count()))
    
    
    //    val ncPersonData = getPersonData(ncProcData)
//    val flPersonData = getPersonData(flProcData)
//    
//    val ncKeyVal = ncPersonData.map { x => 
//      ((x.sex, x.age, x.race, x.party, x.phone),(x.fn, x.mn, x.ln))
//    }
//    
//    val flKeyVal = flPersonData.map { x =>
//      ((x.sex, x.age, x.race, x.party, x.phone),(x.fn, x.mn, x.ln))
//    }
//    
//    val approxNameJoin = ncKeyVal.join(flKeyVal).filter{kv =>
//     val name1 = "%s %s %s".format(kv._2._1._1, kv._2._1._2, kv._2._1._3)
//     val name2 = "%s %s %s".format(kv._2._2._1, kv._2._2._2, kv._2._2._3)
//     JaroWinklerMetric.compare(name1, name2).get >= 0.9
//    }
//    
//    println("%d".format(approxNameJoin.count()))
    
  }
  
  def getPersonData(procData: RDD[String]): RDD[Person] = {
    procData.map { x => 
      val tokens = x.split(colSep, -1)
      new Person(tokens(0), tokens(1), tokens(2), tokens(3), tokens(4), tokens(5), tokens(6), tokens(7))
    }
  }
  
  def getDesiredData(data: RDD[String]): RDD[(String, String)] = {
    var desData = data.map{ line =>
      val tokens = line.split(colSep, -1)
      val fn = tokens(0)
      val mn = tokens(1)
      val ln = tokens(2)
      val sex = tokens(3)
      val age = tokens(4)
      val race = tokens(5)
      val party = tokens(6)
      val phone = tokens(7)
    
      ("%s\t%s\t%s\t%s".format(sex, age, race, party),
      "%s %s %s".format(fn, mn, ln))
    }
    
    desData
  }
  
}
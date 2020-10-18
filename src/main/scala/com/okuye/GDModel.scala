package com.okuye

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.io.Source

object GDModel {



  val usage = """
  Usage: parser [-i file] [-o file] [-c sopt] ...
  Where: -i I Set input file to I
         -f F Set output file to F
         -c C Set aws Credentials file to C
  """

//  val spark: SparkSession = SparkSession
//    .builder()
//    .master("local[1]")
//    .appName("okuye.com")
//    .getOrCreate()
//
//  var gdSchema = StructType(
//    Array(
//      StructField("key", IntegerType, true),
//      StructField("value", IntegerType, true)
//    ))
//
//  //  def chooseDataType(opType:String,fullPath:String,header:Boolean, delimiter:String):DataFrame =  opType match {
//  //    case  "csv" => getData(fullPath,",")
//  //    case  "tab" => getData(fullPath,"\t")
//  //  }
//
//  def getData1(fullPath: String, header: Boolean, delimiter: String) =
//    spark.read
//      .format("csv")
//      . // Use "csv" regardless of TSV or CSV.
//      option("header", header)
//      . // Does the file have a header line?
//      option("delimiter", delimiter)
//      . // Set delimiter to tab or comma.
//      schema(gdSchema)
//      . // Schema that was built above.
//      load(fullPath)

//  def getData(fullPath: String, delimiter: String): Unit = {
//    spark.read.option("sep", delimiter).csv(fullPath)
//  }

  def getOddOccurrence(A: Array[Int]): Int = {
    val ar: Array[Int] = Array.ofDim[Int](A.toList.max + 1)
    for (i <- A.indices) ar(A(i)) = ar(A(i)) + 1
    for (i <- ar.indices) if (ar(i) % 2 == 1) return i
    -1
  }

  def die(msg: String = usage) = {
    println(msg)
    sys.exit(1)
  }

  def getCredentials(credentialsPath: String): scala.collection.mutable.Map[String,String] = {
    // create an empty map
    var credentials = scala.collection.mutable.Map[String, String]()
    val list = Source.fromFile(credentialsPath).getLines().slice(1, 3).toList.map(_.split("="))
    // add elements with +=
    credentials += ("aws_access_key_id" -> list(0)(1))
    credentials += ("aws_secret_access_key" -> list(1)(1))
    credentials
  }

  def getFileType(path:String): String ={
    val result = path.substring(path.lastIndexOf('.'))
    result.toLowerCase
  }

  def getDelimiter(extType:String):String= extType match {
    case "csv" => ","
    case "tsv" => "\t"
  }



}

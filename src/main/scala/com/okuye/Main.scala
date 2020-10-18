//https://github.com/ticofab/codility-scala-lessons/blob/master/src/arrays/OddOccurrencesInArray.sc

package com.okuye

import org.apache.spark.sql
import java.util.Properties
import org.apache.spark.sql.DataFrame
import scala.io.Source
import com.typesafe.config.{Config, ConfigFactory}

import GDModel._

//import com.typesafe.config._//  {Config, ConfigFactory, ConfigValueFactory}
object Main {

  val usage =
    """
  Usage: parser [-i file] [-o file] [-c sopt] ...
  Where: -i I Set input file to I
         -f F Set output file to F
         -c C Set aws Credentials file to C
  """

  def main(args: Array[String]): Unit = {

    args.foreach(arg => if (arg.length == 0) die())
    val inputPath = args(0)
    val outputPath = args(1)
    val awsCredentials = getCredentials(args(2))
    val aws_access_key_id = awsCredentials("aws_access_key_id")
    val aws_secret_access_key = awsCredentials("aws_secret_access_key")

    import org.apache.spark.sql.SparkSession

    lazy val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("okuye.com")
      .getOrCreate()

    /*
    val spark: SparkSession = SparkSession.builder()
  .master("local[1]")
  .appName("SparkByExamples.com")
  .getOrCreate()
     */

    val extType = getDelimiter(getFileType(inputPath))
    //    val segments = spark.read.option("sep", ",").option("header","false").csv("/Users/olakunlekuye/Interview/Convex-Test/o2.csv")
    val segments = spark.read.option("sep", extType).option("header", "false").csv(inputPath)
      .na.fill(0)

    val header = segments.first()

    val temData = segments.filter(line => line != header)

    val temData_01 = temData.withColumnRenamed(temData.columns(0), "key").withColumnRenamed(temData.columns(1), "value")
    val temData_02 = temData_01.withColumn("key", temData_01("key").cast(sql.types.IntegerType)).withColumn("value", temData_01("value").cast(sql.types.IntegerType))

    // Creates a temporary view using the DataFrame
    temData_02.createOrReplaceTempView("temData_02")

    val distinctKeys = spark.sql("select distinct key from temData_02 ")

    import spark.implicits._
    val mergedDf = distinctKeys.as("d1").join(temData_02.as("d2"), ($"d1.key" === $"d2.value")).select($"d1.key", $"d2.value")

    var tempKeyVals = scala.collection.mutable.Map[Int, Int]()


    val tDa = distinctKeys.collect()
    for (row <- tDa) {

      val tempVal = row.getInt(0)

      print("\n\n\n")
      print("\n\n\n")
      print("\n\n\n")
      println(tempVal)
      print("\n\n\n")
      print("\n\n\n")
      print("\n\n\n")

      val dfCorruptData = mergedDf.filter("value = " + tempVal)

      val singleCol = dfCorruptData.select("value").map(_.getInt(0)).collect()

      tempKeyVals += (tempVal -> getOddOccurrence(singleCol))

      print("\n\n\n")
      print("\n\n\n")
      print("\n\n\n")
      dfCorruptData.show(false)
      print("\n\n\n")
      print("\n\n\n")
      print("\n\n\n")


      val arr1: Array[Int] = Array(2, 2, 3, 3, 2, 2, 3)
      val arr2: Array[Int] = Array(10, 20, 30, 11, 11, 21)
      val arr3: Array[Int] = Array(10, 20, 30, 11, 11, 21, 21)
      val arr4: Array[Int] = Array(2)
      val arr5: Array[Int] = Array(2, 2, 2)

      print("\nThe element with odd occurrences in arr1 is: " + getOddOccurrence(arr1))
      print("\nThe element with odd occurrences in arr2 is: " + getOddOccurrence(arr2))
      print("\nThe element with odd occurrences in arr3 is: " + getOddOccurrence(arr3))
      print("\nThe element with odd occurrences in arr4 is: " + getOddOccurrence(arr4))
      print("\nThe element with odd occurrences in arr5 is: " + getOddOccurrence(arr5))

    }


    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", aws_access_key_id)
    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", aws_secret_access_key)
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")


    tempKeyVals.toSeq.toDF().show(false)

  }


}
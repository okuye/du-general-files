package com.okuye

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.io.Source

object GDModel {


  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("okuye.com")
    .getOrCreate()

  val usage =
    """
  Usage: parser [-i file] [-o file] [-c sopt] ...
  Where: -i I Set input file to I
         -f F Set output file to F
         -c C Set aws Credentials file to C
  """


  def getOddOccurrence(A: Array[Int]): Int = {
    val ar: Array[Int] = Array.ofDim[Int](A.toList.max + 1)
    for (i <- A.indices) ar(A(i)) = ar(A(i)) + 1
    for (i <- ar.indices) if (ar(i) % 2 == 1) return i
    -1
  }

  //Use this to shut the application down if all the required parameters are not supplied
  def die(msg: String = usage) = {
    println(msg)
    sys.exit(1)
  }

  def getCredentials(credentialsPath: String): scala.collection.mutable.Map[String, String] = {
    // create an empty map
    var credentials = scala.collection.mutable.Map[String, String]()
    val list = Source.fromFile(credentialsPath).getLines().slice(1, 3).toList.map(_.split("="))
    // add elements with +=
    credentials += ("aws_access_key_id" -> list(0)(1))
    credentials += ("aws_secret_access_key" -> list(1)(1))
    credentials
  }

  def getFileType(path: String): String = {
    val result = path.substring(path.lastIndexOf('.') + 1)
    result.toLowerCase
  }

  def getDelimiter(extType: String): String = extType match {
    case "csv" => ","
    case "tsv" => "\t"
  }

  //Return the count of data from an s3 directory
  def returnDataCount(opType: String, inputPath: String, aws_access_key_id: String, aws_secret_access_key: String): Long = {

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", aws_access_key_id)
    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", aws_secret_access_key)
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    //Load a datafrane with the content of the file and return the count
    val df = spark.read.option("sep", getDelimiter(opType)).option("header", "false").csv(inputPath)

    df.count()

  }

  def writeData(opType: String, inputPath: String, outputPath: String, aws_access_key_id: String, aws_secret_access_key: String): Unit = {
    try {

      //Deteremine the extension type of the file
      val extType = getDelimiter(getFileType(inputPath))

      //Load a datafrane with the content of the file and replace all null values with 0
      val tempDF = spark.read.option("sep", extType).option("header", "false").csv(inputPath)
        .na.fill(0)

      //Determine and Remove the inconsistent header(s) present in the data
      val header = tempDF.first()

      val tempDataNoHeader = tempDF.filter(line => line != header)

      //Rename the inconsistent columns to key and value
      val tempDataNoHeader_01 = tempDataNoHeader.withColumnRenamed(tempDataNoHeader.columns(0), "key")
        .withColumnRenamed(tempDataNoHeader.columns(1), "value")
      val tempDataNoHeader_02 = tempDataNoHeader_01.withColumn("key", tempDataNoHeader_01("key")
        .cast(sql.types.IntegerType)).withColumn("value", tempDataNoHeader_01("value")
        .cast(sql.types.IntegerType))

      // Creates a temporary view using the DataFrame
      tempDataNoHeader_02.createOrReplaceTempView("tempDataNoHeader_02")

      //Determine the distint keys in the data
      val distinctKeys = spark.sql("select distinct key from tempDataNoHeader_02")

      //Drop the key column as is not required to filter the data
      val tempDataNoHeader_03 = tempDataNoHeader_02.drop("key")

      import spark.implicits._

      //Create a collection to hold the key value pairs
      var tempKeyValuesMap = scala.collection.mutable.Map[Int, Int]()

      //Iterate over the distinct values in order to determine the odd occurrences in the main dataset
      val tDa = distinctKeys.collect()
      for (row <- tDa) {

        //Get each unique key from the distinct keys in the data
        val tempVal = row.getInt(0)

        val keyFilteredData = tempDataNoHeader_03.filter("value = " + tempVal)

        val keyFilteredDataArray = keyFilteredData.select("value").map(_.getInt(0)).collect()

        //Create a map of a key and value that has odd occurences in the value column
        tempKeyValuesMap += (tempVal -> getOddOccurrence(keyFilteredDataArray))


      }


      spark.sparkContext
        .hadoopConfiguration.set("fs.s3a.access.key", aws_access_key_id)
      // Replace Key with your AWS secret key (You can find this on IAM
      spark.sparkContext
        .hadoopConfiguration.set("fs.s3a.secret.key", aws_secret_access_key)
      spark.sparkContext
        .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

      //Convert the collection into a dataframe to make saving easier
      val df = tempKeyValuesMap.toSeq.toDF()

      opType.toLowerCase match {
        case "csv" =>
          //Save the output to a directory
          df.coalesce(1)
            .write
            .mode("overwrite")
            .option("sep", ",")
            .option("encoding", "UTF-8")
            .csv(outputPath)
        case "tsv" =>
          //Save the output to a directory
          df.coalesce(1)
            .write
            .mode("overwrite")
            .option("sep", "\t")
            .option("encoding", "UTF-8")
            .csv(outputPath)
        case _ => println("The options to save are either csv or tsv")

      }
    }
    catch {
      case e: Exception => e.printStackTrace
    }

  }
}

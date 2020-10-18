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



  // example of how system properties override; note this
  // must be set before the config lib is used
//  System.setProperty("simple-lib.whatever", "This value comes from a system property")
//
//  // Load our own config values from the default location, application.conf
//  val conf = ConfigFactory.load()
//  println("The answer is: " + conf.getString("du-general-app.answer"))

  /*
    answer=42
  input-path="s3bucketname"
  output-path="s3:bucketname"
  aws-credentials= "~/.aws/credentials"
  no-of-processors=2
  application-name="aws-files"
   */

  // In this simple app, we're allowing SimpleLibContext() to
  // use the default config in application.conf ; this is exactly
  // the same as passing in ConfigFactory.load() here, so we could
  // also write "new SimpleLibContext(conf)" and it would be the same.
  // (simple-lib is a library in this same examples/ directory).
  // The point is that SimpleLibContext defaults to ConfigFactory.load()
  // but also allows us to pass in our own Config.
//  val context = new SimpleLibContext()
//  context.printSetting("simple-lib.foo")
//  context.printSetting("simple-lib.hello")
//  context.printSetting("simple-lib.whatever")

//  val config: Config = ConfigFactory.load("application.conf")
//  val driver = config.getString("jdbc.driver")
//  println(s"driver =   $driver")

//  val config = ConfigFactory.load("application.conf").getString("my.secret.value") //.getConfig("okuye")
//    .getConfig("com")
//  val sparkConfig = config.getConfig("spark.app-name")
//  val mysqlConfig = config.getConfig("mysql")
//  val value = ConfigFactory.load().getString("okuye.spark.app-name")
//  val appName = config.getString("okuye.spark.app-name")
//  println(value)

//  val value = ConfigFactory.load().getString("my.secret.value")
//  println(s"My secret value is $config")
//
//  val url = getClass.getResource("aws.properties")
//  val properties: Properties = new Properties()
//
//  val logger = Logger("Root")
//
//  if (url != null) {
//    val source = Source.fromURL(url)
//    properties.load(source.bufferedReader())
//  }
//  else {
//    logger.error("properties file cannot be loaded at path ")
//    throw new FileNotFoundException("Properties file cannot be loaded")
//  }


  val usage = """
  Usage: parser [-i file] [-o file] [-c sopt] ...
  Where: -i I Set input file to I
         -f F Set output file to F
         -c C Set aws Credentials file to C
  """

  def main(args: Array[String]): Unit = {

    args.foreach(arg => if (arg.length == 0) die())
    val inputPath = args(0)
    val outputPath =  args(1)
    val awsCredentials = getCredentials(args(2))
    val aws_access_key_id = awsCredentials("aws_access_key_id")
    val aws_secret_access_key = awsCredentials("aws_secret_access_key")

    import org.apache.spark.sql.SparkSession

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("okuye.com")
      .getOrCreate()

//    val segments = spark.read.option("sep", ",").option("header","false").csv("/Users/olakunlekuye/Interview/Convex-Test/o2.csv")
    val segments = spark.read.option("sep", ",").option("header","false").csv(inputPath)
      .na.fill(0)

    val header = segments.first()

    val temData = segments.filter(line => line != header)

    val temData_01 = temData.withColumnRenamed(temData.columns(0),"key").withColumnRenamed(temData.columns(1),"value")
    val temData_02 = temData_01.withColumn("key",temData_01("key").cast(sql.types.IntegerType)).withColumn("value",temData_01("value").cast(sql.types.IntegerType))

    temData_02.printSchema()
    temData_02.show(false)
    // Creates a temporary view using the DataFrame
    temData_02.createOrReplaceTempView("temData_02")

    val distinctKeys = spark.sql("select distinct key from temData_02 ")

    import spark.implicits._
    val mergedDf = distinctKeys.as("d1").join(temData_02.as("d2"), ($"d1.key" === $"d2.value")).select($"d1.key", $"d2.value")

    var tempKeyVals = scala.collection.mutable.Map[Int, Int]()


    val tDa = distinctKeys.collect()
    for( row <- tDa)
    {

      val tempVal = row.getInt(0)

      print("\n\n\n")
      print("\n\n\n")
      print("\n\n\n")
      println(tempVal)
      print("\n\n\n")
      print("\n\n\n")
      print("\n\n\n")

      val dfCorruptData = mergedDf.filter("value = "+tempVal)

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
          val arr5: Array[Int] = Array(2,2,2)

          print("\nThe element with odd occurrences in arr1 is: " + getOddOccurrence(arr1))
          print("\nThe element with odd occurrences in arr2 is: " + getOddOccurrence(arr2))
          print("\nThe element with odd occurrences in arr3 is: " + getOddOccurrence(arr3))
          print("\nThe element with odd occurrences in arr4 is: " + getOddOccurrence(arr4))
          print("\nThe element with odd occurrences in arr5 is: " + getOddOccurrence(arr5))

      /*
      The element with odd occurrences in arr1 is: 3
The element with odd occurrences in arr2 is: 10
The element with odd occurrences in arr3 is: 10
The element with odd occurrences in arr4 is: 2
The element with odd occurrences in arr5 is: 2
       */
    }

    println(tempKeyVals)

  }



}
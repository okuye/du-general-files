package com.okuye

import com.okuye.GDModel._

object Main {

  //  val usage =
  //    """
  //  Usage: parser [-i file] [-o file] [-c sopt] ...
  //  Where: -i I Set input file to I
  //         -f F Set output file to F
  //         -c C Set aws Credentials file to C
  //  """


  def main(args: Array[String]): Unit = {

    args.foreach(arg => if (arg.length == 0) die()) //Exit the application if all the parameters are not supplied

    val inputPath = args(0)
    val outputPath = args(1)

    println("The inputPath ola is: " + inputPath)
    println("The outputPath ola is: " + outputPath)
    println("The args(2) ola is: " + args(2))
    val awsCredentials = getCredentials(args(2))
    val aws_access_key_id = awsCredentials("aws_access_key_id")
    val aws_secret_access_key = awsCredentials("aws_secret_access_key")

    println("The aws_access_key_id ola is: " + aws_access_key_id)
    println("The aws_secret_access_key ola is: " + aws_secret_access_key)

    //Write the output as a csv for a datafie with unique key and value indicating the odd occurrence
    writeData("csv", inputPath, outputPath, aws_access_key_id, aws_secret_access_key)

    /*
        val extType = getDelimiter(getFileType(inputPath))
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
    */

  }


}
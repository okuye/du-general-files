package com.okuye

import com.okuye.GDModel._

object Main {


  def main(args: Array[String]): Unit = {

    try {

      args.foreach(arg => if (arg.length == 0) die()) //Exit the application if all the parameters are not supplied

      val inputPath = args(0)
      val outputPath = args(1)

      val awsCredentials = getCredentials(args(2))
      val aws_access_key_id = awsCredentials("aws_access_key_id")
      val aws_secret_access_key = awsCredentials("aws_secret_access_key")

      //Write the output as a csv for a datafie with unique key and value indicating the odd occurrence
      writeData("csv", inputPath, outputPath, aws_access_key_id, aws_secret_access_key)

    }
    catch {
      case e: Exception => e.printStackTrace
    }

  }


}
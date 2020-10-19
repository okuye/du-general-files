package com.okuye

import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import GDModel._


class GDModel_Test extends Specification {

  // Load our own config values from the default location, application.conf
  val conf = ConfigFactory.load()

  val fileOutputType = conf.getString("du-general-app.file-output-type")
  val inputPath = conf.getString("du-general-app.input-path")
  val outputPath = conf.getString("du-general-app.output-path")
  val awsCredentialsPath = conf.getString("du-general-app.aws-credentials")

  val awsCredentials = getCredentials(awsCredentialsPath)
  val aws_access_key_id = awsCredentials("aws_access_key_id")
  val aws_secret_access_key = awsCredentials("aws_secret_access_key")




  "3 should be the occurrence in the array arr1 " in {

    val arr1: Array[Int] = Array(2, 2, 3, 3, 2, 2, 3)
    getOddOccurrence(arr1) must be equalTo 3
  }

  "10 should be the occurrence in the array arr2 " in {
    val arr2: Array[Int] = Array(10, 20, 30, 11, 11, 21)
    getOddOccurrence(arr2) must not equalTo 3
  }




  "Write and Read in a csv with multiple occurring numbers " in {

    val mainArray: Array[String] = Array(inputPath,outputPath,awsCredentialsPath)

    Main.main(mainArray)

    returnDataCount(fileOutputType,outputPath,aws_access_key_id,aws_secret_access_key) must be equalTo 5
  }

}

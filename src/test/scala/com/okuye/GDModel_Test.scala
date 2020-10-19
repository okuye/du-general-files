package com.okuye

import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification


class GDModel_Test extends Specification {

  // example of how system properties override; note this
  // must be set before the config lib is used
  System.setProperty("simple-lib.whatever", "This value comes from a system property")

  // Load our own config values from the default location, application.conf
  val conf = ConfigFactory.load()
  //  println("The answer is: " + conf.getString("du-general-app.answer"))


  val inputPath = conf.getString("du-general-app.input-path")
  val outputPath = conf.getString("du-general-app.output-path")
  val awsCredentials = conf.getString("du-general-app.aws-credentials")

  val resourcesPath = getClass.getResource("/t2.tsv")
  println(resourcesPath.getPath)
  println("The inputPath is: " + inputPath)
  println("The outputPath is: " + outputPath)
  println("The awsCredentials is: " + awsCredentials)


  "3 should be the occurrence in the array arr1 " in {
    val arr1: Array[Int] = Array(2, 2, 3, 3, 2, 2, 3)
    GDModel.getOddOccurrence(arr1) must be equalTo 3
  }

  "10 should be the occurrence in the array arr2 " in {
    val arr2: Array[Int] = Array(10, 20, 30, 11, 11, 21)
    GDModel.getOddOccurrence(arr2) must not equalTo 3
  }

  "Read in a csv with multiple occurring numbers " in {

    val args = Array("/Users/olakunlekuye/Dev/du-general-files/t2.tsv", "s3a://olakunlekuye/data", "/Users/olakunlekuye/.aws/credentials")

    Main.main(args) must be equalTo 0
  }

}

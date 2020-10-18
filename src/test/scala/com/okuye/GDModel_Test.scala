package com.okuye

import java.io.FileNotFoundException
import java.util.Properties

import com.typesafe.config.ConfigFactory

//import com.typesafe.scalalogging.Logger
import org.specs2.mutable.Specification

import scala.io.Source


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

  "A missing parameter exception should be thrown " in {

    val path = getClass.getResource("/resources/t1.tsv")

    val folder = new File(path.getPath)

    var args = Array(inputPath, outputPath, "")

    //   val credentialPath = scala.io.Source.fromResource("aws.properties")
    Main.main(args) must equalTo sys.exit(1)
  }


  /*
       val testCSV = Array(
        "1,9002525,,,2,C++",
        "2,9003401,,9002525,4,",
        "2,9003942,,9002525,1,",
        "2,9005311,,9002525,0,",
        "1,28903923,,,0,PHP",
        "2,28904080,,28903923,0,")

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
     */

}

package com.github.amchang.ohdsi

import com.github.amchang.ohdsi.lib.{ConditionEra, DoseEra}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Start point for the entire program
  */
object Main {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val r = (t1 - t0) / (10 * 10^9)
    println("Elapsed time: " + r + "s")
    result
  }

  /**
    * Entry point to the entire program
    * @param args Command line arguments either from the java vm or sbt run
    *             See the README.me for all command line arguments, or application.conf
    */
  def main(args: Array[String]) {
    val sparkConfig = new SparkConf()
      .setAppName("era")
      .setMaster("local[8]")
    implicit val sparkContext = new SparkContext(sparkConfig)

   // new ConditionEra().build
    new DoseEra().build

    // stop the spark context
    sparkContext.stop()
  }

}
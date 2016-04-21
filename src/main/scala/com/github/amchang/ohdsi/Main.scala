package com.github.amchang.ohdsi

import com.github.amchang.ohdsi.lib.{ConditionEra, DoseEra, DrugEra}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Start point for the entire program
  */
object Main {

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    val r = (t1 - t0) / 1000
    println("Elapsed time: " + r + "s")
    result
  }

  /**
    * Entry point to the entire program
    * @param args Command line arguments either from the java vm or sbt run
    *             See the README.me for all command line arguments, or application.conf
    */
  def main(args: Array[String]) {
    // default this
    val level = Level.WARN
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)

    val sparkConfig = new SparkConf()
      .setAppName("era")
      .setMaster("local")
    implicit val sparkContext = new SparkContext(sparkConfig)
    implicit val config = ConfigFactory.load()

    // all of the different eras
    val conditionEra = new ConditionEra()
    conditionEra.build

    val doseEra = new DoseEra()
    doseEra.build

    val drugEraNonStockpile = new DrugEra()
    drugEraNonStockpile.build()

    val drugEraStockpile = new DrugEra()
    drugEraStockpile.build(true)

    // do we need to write csvs
    if (config.getBoolean("ohdsi.csv.enabled")) {
      println(s"writing condition era at ${conditionEra.writeCSV.get}")
      println(s"writing dose era at ${doseEra.writeCSV.get}")
      println(s"writing drug era non stockpile at ${drugEraNonStockpile.writeCSV.get}")
      println(s"writing drug era stockpile at ${drugEraStockpile.writeCSV.get}")
    }

    // stop the spark context
    sparkContext.stop()
  }

}
package com.github.amchang.ohdsi

import com.github.amchang.ohdsi.lib.{ConditionEra, DoseEra, DrugEra}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Start point for the entire program
  */
object Main {

  /**
    * Time to measure performance
    *
    * @param era the era being measured
    * @param block the code block to measure
    * @tparam R Type return of code block
    * @return R Return of R Type
    */
  def time[R](era: String, block: => R): R = {
    val t0 = System.currentTimeMillis().toDouble
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis().toDouble
    val r = (t1 - t0) / 1000.toDouble

    println(s"${era} elapsed time: ${r} s")

    result
  }

  /**
    * Entry point to the entire program
    *
    * @param args Command line arguments either from the java vm or sbt run
    *             See the README.me for all command line arguments, or application.conf
    */
  def main(args: Array[String]) {
    // default this
    val level = Level.WARN
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)

    // https://issues.apache.org/jira/browse/SPARK-12675, bit cache need local to [*]
    val sparkConfig = new SparkConf()
      .setAppName("era")
      .setMaster("local[*]")
      .set("spark.executor.heartbeatInterval", "5m")
      .set("spark.driver.memory", "8g")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.maxResultSize", "0")
    implicit val sparkContext = new SparkContext(sparkConfig)
    implicit val config = ConfigFactory.load()

    // we benchmark the second build so any cache is built up and
    // spark is warmed up

    // all of the different eras
    val conditionEra: ConditionEra = new ConditionEra()
    conditionEra.build
    time("conditionEra", {
      conditionEra.build
    })

    val doseEra: DoseEra = new DoseEra()
    doseEra.build
    time("doseEra", {
      doseEra.build
    })

    val drugEraNonStockpile: DrugEra = new DrugEra()
    drugEraNonStockpile.build()
    time("drugEraNonStockpile", {
      drugEraNonStockpile.build()
    })

    val drugEraStockpile: DrugEra =  new DrugEra()
    drugEraStockpile.build(true)
    time("drugEraStockpile", {
      drugEraStockpile.build(true)
    })

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
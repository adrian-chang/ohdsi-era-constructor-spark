package com.github.amchang.ohdsi.lib

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Mixin all of our Spark related items here
  */
trait Spark {

  /**
    * Hold a reference to the spark context here,
    * make it configurable in the future
    */
  protected val sparkConfig = new SparkConf()
    .setAppName("aphrodite")
    .setMaster("local[8]")

  /**
    * General spark context
    */
  protected val sparkContext = new SparkContext(sparkConfig)

  /**
    * General sql context
    */
  protected val sqlContext = new SQLContext(sparkContext)

  /**
    * Get the config
    */
  protected val config = ConfigFactory.load()

  /**
    * Get a csv reader
    */
  protected def csvReader: DataFrameReader = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("mode", "DROPMALFORMED")
      .option("delimiter", "\t")
      .option("quote", null)
  }

  /**
    * Get the vocab path for a csv
    * @param file the file to get
    * @return full path
    */
  protected def getVocabDir(file: String): String = {
    config.getString("vocab.dir") + file
  }

}

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
    .setAppName("era")
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
    * Generic csv reader to vocab or data reader
    * @return a dataframe reader for csvs
    */
  private def csvReader: DataFrameReader = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("mode", "DROPMALFORMED")
      .option("quote", null)
  }

  /**
    * Data reader for vocab
    * @return data reader configured for vocab
    */
  protected def csvVocabReader: DataFrameReader = {
    csvReader
      .option("delimiter", "\t")
  }

  /**
    * Data reader csv
    * @return reader for data configured correctly
    */
  protected def csvDataReader: DataFrameReader = {
    csvReader
  }

  /**
    * Get the vocab path for a csv
    * @param file the file to get
    * @return full path to a vocab file
    */
  protected def getVocabFile(file: String): String = {
    config.getString("ohdsi.vocab") + file
  }

  /**
    * Get the data path for a csv
    * @param file the file to get
    * @return full path to a vocab file
    */
  protected def getDataFile(file: String): String = {
    config.getString("ohdsi.data") + file
  }

}

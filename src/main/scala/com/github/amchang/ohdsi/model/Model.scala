package com.github.amchang.ohdsi.model

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Future

/**
  * Root trait which all models should use
  */
trait Model {

  /**
    * Hold a reference to the spark context here,
    * make it configurable in the future
    */
  private val sparkConfig = new SparkConf()
    .setAppName("ohdsi")
    .setMaster("local[2]")

  private val sparkContext = new SparkContext(sparkConfig)

  /**
    * The intended replacement route for the project
    * @return
    */
  def stats: Future[String]

  /**
    * Load a table from a database
    * @param tableName the table to load
    * @return a dataframe of the corresponding table
    */
  protected def loadTable(tableName: String): DataFrame ={
    val sc = sparkContext
    val sqlContext = new SQLContext(sc)
    sqlContext
      .read
      .format("jdbc")
      // have this read in options in the future
      .options(
        Map(
          "url" -> "jdbc:postgresql://localhost:5432/ohdsi",
          "driver" -> "org.postgresql.Driver",
          "dbtable" -> tableName
        )).load()
  }

}

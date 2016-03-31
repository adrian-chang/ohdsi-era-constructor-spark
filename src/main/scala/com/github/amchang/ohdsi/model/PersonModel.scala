package com.github.amchang.ohdsi.model

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Manage everything related to dealing with people
  */
object PersonModel {

  def process = {
    val conf = new SparkConf().setAppName("ohdsi").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map(
        "url" -> "jdbc:postgresql://localhost:5432/ohdsi",
        "driver" -> "org.postgresql.Driver",
        "dbtable" -> "person"
      )).load()

    println(jdbcDF.rdd.count())
  }

}

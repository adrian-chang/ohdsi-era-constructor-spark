package com.github.amchang.ohdsi.model

import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.{ExecutionContext, Future}

import ExecutionContext.Implicits.global

/**
  * Manage everything related to dealing with people
  */
object PersonModel extends Model {

  /**
    * What table are we looking in the database for
    */
  private val table = "person"

  /**
    * Get the stats her like iris
    * @return a future json object to return the results or an error
    */
  def stats: Future[Option[String]] = {
    Future {
      val df = loadTable(table)
      df.count().toString
    }
  }

}

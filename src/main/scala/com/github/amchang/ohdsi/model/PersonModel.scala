package com.github.amchang.ohdsi.model

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.{ExecutionContext, Future}

import spray.json._
import DefaultJsonProtocol._

/**
  * Manage everything related to dealing with people
  */
object PersonModel extends Model {

  /**
    * What table are we looking in the database for
    */
  private val table = "person"

  def profile[E](code: => E, t: Long = System.currentTimeMillis()) = (code, System.currentTimeMillis() - t)

  /**
    * Get the count of people
    * @return a future json object to return the results or an error
    */
  def stats: Future[String] = {
    Future {
      val (result, time) = profile {
        val df = loadTable(table)
        df.count()
      }
      time.toString
      Map(
        "result" -> result,
        "executionTime" -> time
      ).toJson.compactPrint
    }
  }

}

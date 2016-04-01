package com.github.amchang.ohdsi.model


import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global

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

      Map(
        "result" -> result,
        "executionTime" -> time
      ).toJson.compactPrint
    }
  }

}

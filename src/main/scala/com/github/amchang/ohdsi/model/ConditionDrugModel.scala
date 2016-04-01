package com.github.amchang.ohdsi.model


import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import spray.json._
import DefaultJsonProtocol._

/**
  * Base condition drug model
  */
object ConditionDrugModel extends Model {

  /**
    * Name of the condition table
    */
  private val conditionTableName = "condition_occurrence"

  private val drugTableName = "drug_exposure"

  /**
    * Get the count of people with a condition occurrence and drug exposure
    * @return
    */
  def stats: Future[String] = {
    Future {
      val (result: Long, time: Long) = profile {
        val conditionTable = loadTable(conditionTableName)
        val drugTable = loadTable(drugTableName)

        // register all of the tables
        conditionTable.registerTempTable(conditionTableName)
        drugTable.registerTempTable(drugTableName)

        conditionTable.sqlContext.sql(
          s"""
            |SELECT count(1) from (
              |SELECT DISTINCT person_id
              |FROM $conditionTableName
              |INTERSECT
              |SELECT DISTINCT person_id
              |FROM $drugTableName
            |) op
          """.stripMargin).first().get(0)
      }

      val seconds: Double = time / 1000.0

      Map(
        "result" -> result.toDouble,
        "executionTime" -> seconds
      ).toJson.compactPrint
    }
  }
}

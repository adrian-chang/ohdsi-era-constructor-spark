package com.github.amchang.ohdsi.lib

import org.apache.spark.rdd.RDD
import com.github.nscala_time.time.Imports._

/**
  * Replicate the functionality within
  * https://github.com/OHDSI/Era-Constructor/blob/master/v5/PostgreSQL/postgres_v5_condition_era.sql
  */
object ConditionEra extends Spark with Era {

  type PersonId = Int
  type ConditionOccurrenceId = Int
  type ConditionConceptId = Int
  type ConditionStartDate = DateTime
  type ConditionEndDate = DateTime
  type EndDate = DateTime
  type Count = Int

  /**
    * Build an entire era
    */
  def build(): RDD[(ConditionConceptId, ConditionConceptId, DateTime, DateTime, Count)] = {
    val conditionOccurrence = csvDataReader.load(getDataFile("CDM_CONDITION_OCCURRENCE.csv")).cache

    conditionOccurrence.map { row =>
      // all of the same fields
      val personId = row.getString(1).toInt
      val formatter = DateTimeFormat.forPattern("yyyyMMdd")
      val conditionConceptId = row.getString(2).toInt
      val conditionStartDate = formatter.parseDateTime(row.getString(3))
      var conditionEndDate: DateTime = null

      // if it's null, day + 1
      if (row.getString(4).isEmpty) {
        // add an extra date
        conditionEndDate = conditionStartDate.plusDays(1)
      } else {
        conditionEndDate = formatter.parseDateTime(row.getString(4))
      }

      ((personId,  conditionConceptId), List((conditionStartDate, conditionEndDate)))
    }
    .reduceByKey(_ ++ _)
    .map {
      case ((personId, conditionConceptId), startDateEndDateList) =>
        var sortedStartEndDateList = startDateEndDateList.sortBy(_._1)
        // the one i'm currently looking at
        var currentRange = sortedStartEndDateList.head
        var currentAmount = 1
        // use the head as the starting point
        sortedStartEndDateList = sortedStartEndDateList diff List(currentRange)
        var finalCombine = List[((DateTime, DateTime), ConditionConceptId)]()

        while (sortedStartEndDateList.nonEmpty) {
          // get the next one
          val (startStartDate, startEndDate) = currentRange
          val (endStartDate, endEndDate) = sortedStartEndDateList.head
          // is it less than 30 days? keep going then
          if (new Interval(startStartDate, startEndDate.plusDays(30)).overlaps(new Interval(endStartDate, endEndDate))) {
            currentRange = (startStartDate, if (endEndDate.isAfter(startEndDate)) endEndDate else startEndDate)
            currentAmount = currentAmount + 1
          } else {
            finalCombine = finalCombine ++ List((currentRange, currentAmount))
            currentAmount = 1
            currentRange = (endStartDate, endEndDate)
          }

          // remove the head
          sortedStartEndDateList = sortedStartEndDateList diff List(sortedStartEndDateList.head)
        }
        // fence case
        finalCombine = finalCombine ++ List((currentRange, currentAmount))

        ((personId, conditionConceptId), finalCombine)
    }.flatMap { item =>
      item._2.map { a =>
        ((item._1._1, item._1._2, a._1._1, a._1._2), a._2)
      }
    }
    .reduceByKey(_ + _)
    .map{ item =>
      (item._1._1, item._1._2, item._1._3, item._1._4, item._2)
    }
    .sortBy({ item =>
      ((item._1, item._2), item._3.getMillis)
    }).take(100).foreach(println(_))
    null
  }

  def buildOld(): RDD[(ConditionConceptId, ConditionConceptId, DateTime, DateTime, ConditionConceptId)] = {
    val conditionOccurrence = csvDataReader.load(getDataFile("CDM_CONDITION_OCCURRENCE.csv")).cache

    val result = conditionOccurrence.map { row =>
      // all of the same fields
      val conditionOccurrenceId = row.getString(0).toInt
      val personId = row.getString(1).toInt
      val formatter = DateTimeFormat.forPattern("yyyyMMdd")
      val conditionConceptId = row.getString(2).toInt
      val conditionStartDate = formatter.parseDateTime(row.getString(3))
      var conditionEndDate: DateTime = null

      // if it's null, day + 1
      if (row.getString(4).isEmpty) {
        // add an extra date
        conditionEndDate = conditionStartDate.plusDays(1)
      } else {
        conditionEndDate = formatter.parseDateTime(row.getString(4))
      }

      val startEndMap = Map("end" -> List(conditionEndDate), "start" -> List(conditionStartDate))

      ((personId,  conditionConceptId, conditionOccurrenceId), startEndMap)
    }.reduceByKey {
      case (first, second) =>
        // we need to gather all of the starts and the ends now
        val end = first.getOrElse("end", List()) ++ second.getOrElse("end", List())
        val start = first.getOrElse("start", List()) ++ second.getOrElse("start", List())

        Map("end" -> end, "start" -> start)
    }.map {
      case ((personId, conditionConceptId, conditionOccurrenceId), startEndMap) =>
        // find the true start and end
        val startList: List[DateTime] = startEndMap.getOrElse("start", List[DateTime]())
        val endList: List[DateTime] = startEndMap.getOrElse("end", List[DateTime]())
        val start: DateTime = startList.sortWith { (first, second) =>
          first.isBefore(second)
        }.head
        val end: DateTime = endList.sortWith{ (first, second) =>
          first.isBefore(second)
        }.last

        // eliminating conditionOccurrenceId, so now have to watch for dups
        ((personId, conditionConceptId, start, end), 1)
    }.reduceByKey(_ + _).map{
      case ((personId, conditionConceptId, start, end), count) =>
        (personId, conditionConceptId, start, end, count)
    }.sortBy{
      case (personId, conditionConceptId, start, end, length) =>
        // sort by their id and concept
        (personId, conditionConceptId, start, end)
    }

    result
  }


  /**
    * Represent the cte condition target tmp table
    *
    * @return Rdd[(conditionOccurrenceId, personId, conditionConceptId, conditionStartDate, conditionEndDate)]
    */
  private def cteConditionTarget: RDD[(ConditionOccurrenceId, PersonId, ConditionConceptId, ConditionStartDate, ConditionEndDate)] = {
    val conditionOccurrence = csvDataReader.load(getDataFile("CDM_CONDITION_OCCURRENCE.csv"))

    conditionOccurrence.map { row =>
      val conditionOccurrenceId = row.getString(0).toInt
      val personId = row.getString(1).toInt
      val formatter = DateTimeFormat.forPattern("yyyyMMdd")
      val conditionConceptId = row.getString(2).toInt
      val conditionStartDate = formatter.parseDateTime(row.getString(3))
      var conditionEndDate: DateTime = null

      // if it's null, day + 1
      if (row.getString(4).isEmpty) {
        // add an extra date
        conditionEndDate = conditionStartDate.plusDays(1)
      } else {
        conditionEndDate = formatter.parseDateTime(row.getString(4))
      }

      (conditionOccurrenceId, personId, conditionConceptId, conditionStartDate, conditionEndDate)
    }.cache
  }

  /**
    * Represent the cte end dates temp table
    *
    * @param cteConditionTarget results from cte condition target
    * @return cteEndDates (condition end dates) RDD[(PersonId, ConditionConceptId, EndDate)]
    */
  private def cteEndDates(cteConditionTarget: RDD[(ConditionOccurrenceId, PersonId, ConditionConceptId,
    ConditionStartDate, ConditionEndDate)]): RDD[(PersonId, ConditionConceptId, EndDate)] = {

    // get the latest end date per key
    cteConditionTarget.map {
      case (conditionOccurrenceId, personId, conditionConceptId, conditionStartDate, conditionEndDate) =>
        // put all of the extra values in a big list
        ((personId, conditionConceptId), List(conditionEndDate))
    }.reduceByKey(_ ++ _).map {
      case ((personId, conditionConceptId), conditionEndDateList) =>
        // get the oldest one
        val oldest = conditionEndDateList.sortWith {
          case (first, second) =>
            first.isBefore(second)
        }

        (personId, conditionConceptId, oldest(0))
    }.sortBy {
      // sort by the entire key
      case (personId, conditionConceptId, endDate) =>
        (personId, conditionConceptId, endDate)
    }.cache
  }

  /**
    * SELECT
        c.person_id
	, c.condition_concept_id
	, c.condition_start_date
	, MIN(e.end_date) AS era_end_date
FROM cteConditionTarget c
JOIN cteEndDates e ON c.person_id = e.person_id AND c.condition_concept_id = e.condition_concept_id AND e.end_date >= c.condition_start_date
GROUP BY
        c.condition_occurrence_id
	, c.person_id
	, c.condition_concept_id
	, c.condition_start_date
)
    */
  /**
    * Find all of the ends
    * @param cteConditionTargetRDD
    * @param cteEndDatesRDD
    */
  private def cteConditionEnds(cteConditionTargetRDD: RDD[(ConditionOccurrenceId, PersonId, ConditionConceptId, ConditionStartDate, ConditionEndDate)],
                               cteEndDatesRDD: RDD[(PersonId, ConditionConceptId, EndDate)]) = {

  }

}

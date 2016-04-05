package com.github.amchang.ohdsi.lib

import org.apache.spark.rdd.RDD
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.Row
import org.joda.time.{Days, Interval}

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
    * Build an entire era for drugs
    *
    * @return RDD[(ConditionConceptId, ConditionConceptId, DateTime, DateTime, Count)], similar to the results from
    *         the comparable sql query
    */
  def build(): RDD[(ConditionConceptId, ConditionConceptId, DateTime, DateTime, Count)] = {
    val conditionOccurrence =
      csvDataReader.load(getDataFile("CDM_CONDITION_OCCURRENCE.csv"))
        .cache

    conditionOccurrence
      .map(mapToPersonIdConceptId)
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
      }
      .flatMap {
        // find the count of net ranges
        case ((personId, conditionConceptId), finalCombine) =>
          finalCombine.map {
            case ((firstDate, secondDate), count) =>
              ((personId, conditionConceptId, firstDate, secondDate), count)
          }
      }
      // get rid of dups
      .reduceByKey(_ + _)
      .map{
        // flatten out everything with the count
        case ((personId, conditionConceptId, startDateEra, endDateEra), count) =>
          (personId, conditionConceptId, startDateEra, endDateEra, count)
      }
      .sortBy {
        // sort by person id, conditionConceptId, and startDateEra desc
        case (personId, conditionConceptId, startDateEra, endDateEra, count) =>
          (personId, conditionConceptId, startDateEra.getMillis * -1)
      }
      .cache
  }

  /**
    * Map each data type into the final format with the correct key
    */
  private val mapToPersonIdConceptId = (row: Row) => {
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

    ((personId, conditionConceptId), List((conditionStartDate, conditionEndDate)))
  }

}

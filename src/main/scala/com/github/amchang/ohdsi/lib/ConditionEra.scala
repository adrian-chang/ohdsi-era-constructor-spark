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
    * Build an entire era for drugs
    *
    * @return RDD[(ConditionConceptId, ConditionConceptId, DateTime, DateTime, Count)], similar to the results from
    *         the comparable sql query
    */
  def build(): RDD[(ConditionConceptId, ConditionConceptId, DateTime, DateTime, Count)] = {
    val conditionOccurrence =
      csvDataReader.load(getDataFile("CDM_CONDITION_OCCURRENCE.csv"))
        .cache

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
      ((item._1._1, item._1._2, item._1._3, item._1._4), item._2)
    }
    .sortByKey()
    
   /* .sortBy({ item =>
      val key: Double = item._3.getMillis
      if (item._1 == 0 && item._2 == 138525) {
        println(item._3)
        println(key)
      }

      ((item._1, item._2), item._3.getMillis)
    }).take(30).foreach(println(_))*/
    null
  }

}

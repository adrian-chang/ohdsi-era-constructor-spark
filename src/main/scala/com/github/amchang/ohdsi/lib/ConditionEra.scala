package com.github.amchang.ohdsi.lib

import org.apache.spark.rdd.RDD
import com.github.nscala_time.time.Imports._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Replicate the functionality within
  *
  * https://github.com/OHDSI/Era-Constructor/blob/master/v5/PostgreSQL/postgres_v5_condition_era.sql
  */
class ConditionEra(implicit sparkCont: SparkContext, conf: Config = ConfigFactory.load()) extends Spark {

  /**
    * Override these from the trait extends
    */
  protected val sparkContext = sparkCont
  protected val sqlContext = new SQLContext(sparkContext)
  protected val config = conf

  type PersonId = Int
  type ConditionOccurrenceId = Int
  type ConditionConceptId = Int
  type ConditionStartDate = DateTime
  type ConditionEndDate = DateTime
  type EndDate = DateTime
  type Count = Int

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

  /**
    * Build an entire era for drugs
    *
    * @return RDD[(ConditionConceptId, ConditionConceptId, DateTime, DateTime, Count)], similar to the results from
    *         the comparable sql query
    */
  def build: RDD[(ConditionConceptId, ConditionConceptId, DateTime, DateTime, Count)] = {
    val conditionOccurrence =
      csvDataReader.load(getDataFile("CDM_CONDITION_OCCURRENCE.csv"))
        .cache

    conditionOccurrence
      .map(mapToPersonIdConceptId)
      .reduceByKey(_ ++ _)
      .map {
        case ((personId, conditionConceptId), startDateEndDateList) =>
          //((personId, conditionConceptId), 1)
          ((personId, conditionConceptId), Era.rangeBuilder(startDateEndDateList, 30))
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

}

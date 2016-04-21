package com.github.amchang.ohdsi.lib

import com.github.nscala_time.time.Imports._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

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
    * Store the most recent build for csv writing
    */
  private var mostRecentBuild: RDD[(ConditionConceptId, ConditionConceptId, DateTime, DateTime, Count)] = null

  /**
    * Build an entire era for drugs
    *
    * @return RDD[(PersonId, ConditionConceptId, DateTime, DateTime, Count)], similar to the results from
    *         the comparable sql query
    */
  def build: RDD[(PersonId, ConditionConceptId, DateTime, DateTime, Count)] = {
    val conditionOccurrence = loadConditionOccurrence

    if (conditionOccurrence.count == 0) {
      return sparkContext.emptyRDD
    }

    mostRecentBuild = conditionOccurrence
      .map(mapToPersonIdConceptId(config.getString("ohdsi.dateFormat")))
      .reduceByKey(_ ++ _)
      .map {
        case ((personId, conditionConceptId), startDateEndDateList) =>
          ((personId, conditionConceptId), Era.rangeBuilder(startDateEndDateList, 30))
      }
      .flatMap {
        // find the count of net ranges
        case ((personId, conditionConceptId), finalCombine) =>
          finalCombine.map {
            case ((firstDate, secondDate), count, noStockpile, stockpile) =>
              ((personId, conditionConceptId, firstDate, secondDate), count)
          }
      }
      // get rid of timelines that are exactly on each other
      .reduceByKey(_ + _)
      .map {
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

    mostRecentBuild
  }

  /**
    * Override the write csv method to write out a csv as necessary
    *
    * @return location of file
    */
  override def writeCSV: Option[String] = {
    if (mostRecentBuild != null) {
      val format = sparkContext.broadcast(config.getString("ohdsi.dateFormat"))
      val rowRdd = mostRecentBuild.zipWithIndex.map {
        case ((personId, conditionConceptId, startDate, endDate, count), index) =>
          Row(index.toString, personId.toString, conditionConceptId.toString, startDate.toString(format.value),
            endDate.toString(format.value), count.toString)
      }.sortBy(_.getString(0))

      val location = s"${config.getString("ohdsi.csv.location")}condition_era_${System.currentTimeMillis()}"
      sqlContext.createDataFrame(rowRdd, StructType(List(
          StructField("condition_occurrence_id", StringType, true),
          StructField("person_id", StringType, true),
          StructField("condition_concept_id", StringType, true),
          StructField("condition_era_start_date", StringType, true),
          StructField("condition_era_end_date", StringType, true),
          StructField("condition_occurrence_count", StringType, true)
        )))
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(location)

      Some(location)
    } else {
      None
    }
  }

  /**
    * Map each data type into the final format with the correct key
    */
  private val mapToPersonIdConceptId = (dateFormat: String) => (row: Row) => {
    // all of the same fields
    val personId = row.getString(1).toInt
    val formatter = DateTimeFormat.forPattern(dateFormat)
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
    * Load the condition occurrence data frame
    *
    * @return the data frame for condition occurrence
    */
  private def loadConditionOccurrence: DataFrame = {
    csvDataReader
      .load(getDataFile(config.getString("ohdsi.data.conditionOccurrence")))
      .cache
  }

}

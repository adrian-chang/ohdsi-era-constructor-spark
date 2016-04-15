package com.github.amchang.ohdsi.lib

import java.nio.file.{Files, Paths}

import com.github.nscala_time.time.Imports._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Replication of functionality in
  *
  * https://github.com/OHDSI/Era-Constructor/blob/master/v5/PostgreSQL/postgres_v5_dose_era.sql
  */
class DoseEra(implicit sparkCont: SparkContext, conf: Config = ConfigFactory.load()) extends DrugStockpile {

  /**
    * Override these from the trait extends
    */
  protected val sparkContext = sparkCont
  protected val sqlContext = new SQLContext(sparkContext)
  protected val config = conf

  /**
    * Build Dosage Eras
    */
  def build: RDD[(PersonId, DrugConceptId, UnitConceptId, DoseValue, DrugExposureStartDate, DrugExposureEndDate)] = {
    // the entire data
    val bareData = createInitialData()

    val result = bareData
      .reduceByKey(_ ++ _)
      .map {
        case ((personId, drugConceptId, unitConceptId, doseValue), dateList) =>
          ((personId, drugConceptId, unitConceptId, doseValue), Era.rangeBuilder(dateList))
      }
      .flatMap {
        // find the count of net ranges
        case (((personId, drugConceptId, unitConceptId, doseValue), finalCombine)) =>
          finalCombine.map {
            case ((firstDate, secondDate), count) =>
              // differs slightly from project, no count
              (personId, drugConceptId, unitConceptId, doseValue, firstDate, secondDate)
        }
      }
      // get rid of dups
      .distinct
      .map{
        // flatten out everything with the count
        case (personId, drugConceptId, unitConceptId, doseValue, firstDate, secondDate) =>
          (personId, drugConceptId, unitConceptId, doseValue, firstDate, secondDate)
      }
      .sortBy {
        // sort by person id, conditionConceptId, and startDateEra desc
        case (personId, drugConceptId, unitConceptId, doseValue, startDate, endDate) =>
          (personId, drugConceptId)
      }
      .cache

    result
  }



}

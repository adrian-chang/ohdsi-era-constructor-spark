package com.github.amchang.ohdsi.lib

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Calculate drug era's using the non stockpile method
  *
  * http://forums.ohdsi.org/t/where-does-gap-days-come-from-when-building-cdm-v5-drug-era/661
  */
class DrugEraNonStockpile(implicit sparkCont: SparkContext, conf: Config = ConfigFactory.load()) extends DrugExposure {

  /**
    * Override these from the trait extends
    */
  protected val sparkContext = sparkCont
  protected val sqlContext = new SQLContext(sparkContext)
  protected val config = conf

  /**
    * Store the most recent build
    */
  private val mostRecentBuild: RDD[(PersonId, DrugConceptId, UnitConceptId, DoseValue, DrugExposureStartDate, DrugExposureEndDate, GapDays)] = null

  /**
    * Build the entire RDD here for a drug era with the non stockpile method
    *
    * @return @return RDD of (PersonId, DrugConceptId, UnitConceptId, DoseValue, DrugExposureStartDate, DrugExposureEndDate, GapDays)
    *         with the non stockpile method
    */
  def build(): RDD[(PersonId, DrugConceptId, UnitConceptId, DoseValue, DrugExposureStartDate, DrugExposureEndDate, GapDays)] = {
    // the entire data
    val bareData = createInitialData()

    // nothing return nothing
    if (bareData.count == 0) {
      return sparkContext.emptyRDD
    }

    mostRecentBuild = bareData
      .reduceByKey(_ ++ _)
      .map {
        // at this rate we have all of the dates for a particular key
        case ((personId, drugConceptId, unitConceptId, doseValue), dateList) =>
          ((personId, drugConceptId, unitConceptId, doseValue), Era.rangeBuilder(dateList))
      }
      .flatMap {
        // find the count of net ranges
        case (((personId, drugConceptId, unitConceptId, doseValue), finalCombine)) =>
          finalCombine.map {
            case ((firstDate, secondDate), count, noStockpile, stockpile)  =>
              // differs slightly from project, no count
              (personId, drugConceptId, unitConceptId, doseValue, firstDate, secondDate, noStockpile)
          }
      }
      // get rid of duplicates
      .reduceByKey(_ + _)
      .map{
        // flatten out everything with the count
        case (personId, drugConceptId, unitConceptId, doseValue, firstDate, secondDate) =>
          (personId, drugConceptId, unitConceptId, doseValue, firstDate, secondDate)
      }
      .sortBy {
        // sort by person id, conditionConceptId, and startDateEra desc
        case (personId, drugConceptId, unitConceptId, doseValue, startDate, endDate) =>
          (personId, drugConceptId, startDate.getMillis * -1)
      }
      .cache

    mostRecentBuild
  }

  /**
    * Write the result of the most recent build,
    * default, it will write nothing
    *
    * @return the string where the file was written to
    */
  override def writeCSV(): Option[String] = {
    if (mostRecentBuild != null) {
      None
    } else {
      None
    }
  }
}

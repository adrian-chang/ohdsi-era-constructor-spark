package com.github.amchang.ohdsi.lib

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Calculate drug era's using the non stockpile / stockpile method
  *
  * http://forums.ohdsi.org/t/where-does-gap-days-come-from-when-building-cdm-v5-drug-era/661
  */
class DrugEra(implicit sparkCont: SparkContext, conf: Config = ConfigFactory.load()) extends DrugExposure {

  /**
    * Override these from the trait extends
    */
  protected val sparkContext = sparkCont
  protected val sqlContext = new SQLContext(sparkContext)
  protected val config = conf

  /**
    * Store the most recent build
    */
  private var mostRecentBuild: RDD[(PersonId, DrugConceptId, DrugExposureStartDate, DrugExposureEndDate, ExposureCount, GapDays)] = null

  /**
    * Store the most recent method used
    */
  private var mostRecentStockUsed: Boolean = false

  /**
    * Build the entire RDD here for a drug era with the non stockpile method
    *
    * @return @return RDD of (PersonId, DrugConceptId, DrugExposureStartDate, DrugExposureEndDate, ExposureCount, GapDays)
    *         with the non stockpile method
    */
  def build(stock: Boolean = false): RDD[(PersonId, DrugConceptId, DrugExposureStartDate, DrugExposureEndDate, ExposureCount, GapDays)] = {
    // the entire data
    val bareData = createInitialData()
    mostRecentStockUsed = stock

    // nothing return nothing
    if (bareData.count == 0) {
      mostRecentBuild = sparkContext.emptyRDD
      return sparkContext.emptyRDD
    }

    val stockSpark = sparkContext.broadcast(stock)

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
              ((personId, drugConceptId, firstDate, secondDate), (count, if (!stockSpark.value) noStockpile else stockpile))
          }
      }
      // get rid of duplicates
      .reduceByKey {
        case((firstOne, firstTwo), (secondOne, secondTwo)) =>
          (firstOne + secondOne, firstTwo + secondTwo)
      }
      .map{
        // flatten out everything with the count
        case ((personId, drugConceptId, firstDate, secondDate), (exposureCount, gapDays)) =>
          (personId, drugConceptId, firstDate, secondDate, exposureCount, gapDays)
      }
      .sortBy {
        // sort by person id, conditionConceptId, and startDateEra desc
        case (personId, drugConceptId, startDate, endDate, exposureCount, gapDays) =>
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
      val format = sparkContext.broadcast(config.getString("ohdsi.dateFormat"))
      val rowRdd = mostRecentBuild.zipWithIndex.map {
        case ((personId, drugConceptId, startDate, endDate, exposureCount, gapDays), index) =>
          Row(index.toString, personId.toString, drugConceptId.toString,
            startDate.toString(format.value), endDate.toString(format.value),
            exposureCount.toString, gapDays.toString)
      }.sortBy(_.getString(0))

      val location = s"${config.getString("ohdsi.csv.location")}dose_era_${if (!mostRecentStockUsed) "non_stockpile" else "stockpile"}_${System.currentTimeMillis()}"

      sqlContext.createDataFrame(rowRdd, StructType(List(
          StructField("drug_era_id", StringType, true),
          StructField("person_id", StringType, true),
          StructField("drug_concept_id", StringType, true),
          StructField("drug_era_start_date", StringType, true),
          StructField("drug_era_end_date", StringType, true),
          StructField("drug_exposure_count", StringType, true),
          StructField("gap_days", StringType, true)
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
}

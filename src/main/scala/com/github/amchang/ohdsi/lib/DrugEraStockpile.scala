package com.github.amchang.ohdsi.lib

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
  * Calculate drug era's using the stockpile method
  *
  * http://forums.ohdsi.org/t/where-does-gap-days-come-from-when-building-cdm-v5-drug-era/661
  */
class DrugEraStockpile(implicit sparkCont: SparkContext, conf: Config = ConfigFactory.load()) extends DrugExposure {

  /**
    * Override these from the trait extends
    */
  protected val sparkContext = sparkCont
  protected val sqlContext = new SQLContext(sparkContext)
  protected val config = conf

}

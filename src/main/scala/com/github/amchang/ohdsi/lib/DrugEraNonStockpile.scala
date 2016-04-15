package com.github.amchang.ohdsi.lib

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext

/**
  * Calculate drug era's using the non stockpile method
  *
  * http://forums.ohdsi.org/t/where-does-gap-days-come-from-when-building-cdm-v5-drug-era/661
  */
class DrugEraNonStockpile(implicit sparkCont: SparkContext, conf: Config = ConfigFactory.load()) {

}

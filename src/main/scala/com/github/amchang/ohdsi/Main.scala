package com.github.amchang.ohdsi

import com.github.amchang.ohdsi.lib.{ConditionEra, DoseEra}
import com.github.amchang.ohdsi.lib.ConditionEra.{ConditionConceptId, Count}

/**
  * Start point for the entire program
  */
object Main {

  /**
    * Entry point to the entire program
    * @param args Command line arguments either from the java vm or sbt run
    *             See the README.me for all command line arguments, or application.conf
    */
  def main(args: Array[String]) {
    //ConditionEra.build
    DoseEra.build
  }

}
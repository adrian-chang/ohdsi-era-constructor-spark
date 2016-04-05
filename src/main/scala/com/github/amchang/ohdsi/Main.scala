package com.github.amchang.ohdsi

import com.github.amchang.ohdsi.lib.ConditionEra
import com.typesafe.config.ConfigFactory

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
    val b = ConditionEra.build
    val c = 1
  }

}
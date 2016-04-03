package com.github.amchang.ohdsi

import com.github.amchang.ohdsi.lib.KeywordList
import com.typesafe.config.{ConfigFactory}

/**
  * Start point for the entire program
  */
object Main {

  def main(args: Array[String]) {
    val name = "pulmonary oil microembolism"
    KeywordList.build(name)
  }

}
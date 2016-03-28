package com.github.amchang.ohdsi

import colossus.IOSystem
import com.github.amchang.ohdsi.service.PersonService
import com.typesafe.config.ConfigFactory

/**
  * Start point for the entire program
  */
object Main {

  def main(args: Array[String]) {
    implicit val io = IOSystem()
    val config = ConfigFactory.load()


    // just for people queries
    PersonService.start(9001)
  }

}
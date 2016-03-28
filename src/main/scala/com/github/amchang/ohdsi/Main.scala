package com.github.amchang.ohdsi

import colossus.IOSystem
import com.github.amchang.ohdsi.service.PersonService

/**
  * Start point for the entire program
  */
object Main {

  def main(args: Array[String]) {
    implicit val io = IOSystem()

    // just for people queries
    PersonService.start(9001)
  }

}
package com.github.amchang.ohdsi.lib

import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

/**
  * Root Spec for all things RDD
  */
abstract class RDDSpec extends FunSpec with BeforeAndAfterAll {

  override protected def beforeAll() = {
    val level = Level.WARN
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
  }

}

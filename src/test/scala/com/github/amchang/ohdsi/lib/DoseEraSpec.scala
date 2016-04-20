package com.github.amchang.ohdsi.lib

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}

/**
  * Test out the DoseEra class
  */
class DoseEraSpec extends FunSpec with BeforeAndAfter with MockitoSugar with BeforeAndAfterAll {

  // setup values
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("dose_era_spec")
    .setMaster("local")
  implicit val conf = mock[Config]
  implicit val sparkCont: SparkContext = new SparkContext(sparkConf)
  val sqlCont: SQLContext = new SQLContext(sparkCont)

  var doseEra: DoseEra = null
  var doseEraData: List[((Int, Int, String, String), List[(DateTime, DateTime)])] = List()
  val dateStringFormat = "yyyyMMdd"
  val dateStringFormatter = DateTimeFormat.forPattern(dateStringFormat)
  when(conf.getString("ohdsi.dateFormat")).thenReturn(dateStringFormat)

  before {
    doseEra = new DoseEra()

    val createInitialData = classOf[DrugExposure].getDeclaredField("createInitialData")
    createInitialData.setAccessible(true)
    // stub the data here
    createInitialData.set(doseEra, () => {
      sparkCont.parallelize(doseEraData)
    }: RDD[((Int, Int, String, String), List[(DateTime, DateTime)])])
  }

  describe("build") {
    it("returns an empty RDD") {
      assert(doseEra.build.isEmpty)
    }

    /*it("returns a singular RDD") {
      conditionOccurrenceData = List(
        Row("165361", "0", "138525", "20100630", "20100630")
      )

      val result = conditionEra.build.collect

      assert(result.length == 1)
      assert(result(0) ==(0, 138525, dateStringFormatter.parseDateTime("20100630"), dateStringFormatter.parseDateTime("20100630"), 1))
    }

    it("returns an RDD with two non overlapping ranges") {
      conditionOccurrenceData = List(
        Row("165361", "0", "138525", "20100630", "20100630"),
        Row("178861", "0", "138525", "20091203", "20091203")
      )

      val result = conditionEra.build.collect

      assert(result.length == 2)
      assert(result(0) ==(0, 138525, dateStringFormatter.parseDateTime("20100630"), dateStringFormatter.parseDateTime("20100630"), 1))
      assert(result(1) ==(0, 138525, dateStringFormatter.parseDateTime("20091203"), dateStringFormatter.parseDateTime("20091203"), 1))
    }

    it("returns an RDD with two ranges, one overlapping") {
      conditionOccurrenceData = List(
        Row("165361", "0", "138525", "20100630", "20100630"),
        Row("142861", "0", "138525", "20100609", "20100609"),
        Row("178861", "0", "138525", "20091203", "20091203")
      )

      val result = conditionEra.build.collect

      assert(result.length == 2)
      assert(result(0) ==(0, 138525, dateStringFormatter.parseDateTime("20100609"), dateStringFormatter.parseDateTime("20100630"), 2))
      assert(result(1) ==(0, 138525, dateStringFormatter.parseDateTime("20091203"), dateStringFormatter.parseDateTime("20091203"), 1))
    }

    it("returns an RDD with three ranges, one overlapping") {
      conditionOccurrenceData = List(
        Row("165361", "0", "138525", "20100630", "20100630"),
        Row("142861", "0", "138525", "20100609", "20100609"),
        Row("178861", "0", "138525", "20091203", "20091203"),
        Row("208861", "1", "138525", "20120104", "20120201")
      )

      val result = conditionEra.build.collect

      assert(result.length == 3)
      assert(result(0) ==(0, 138525, dateStringFormatter.parseDateTime("20100609"), dateStringFormatter.parseDateTime("20100630"), 2))
      assert(result(1) ==(0, 138525, dateStringFormatter.parseDateTime("20091203"), dateStringFormatter.parseDateTime("20091203"), 1))
      assert(result(2) ==(1, 138525, dateStringFormatter.parseDateTime("20120104"), dateStringFormatter.parseDateTime("20120201"), 1))
    }*/
  }

  describe("writeCSV") {
    it("does nothing considering it has nothing to write") {
      intercept[NullPointerException] {
     //   conditionEra.writeCSV
      }
    }

    it("writes a csv file out") {
      when(conf.getString("ohdsi.csv.location")).thenReturn("/tmp/")

  /*    conditionEra.build
      conditionEra.writeCSV*/
    }

  }
}
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

    it("returns a singular RDD") {
      val firstDate = "20080605"
      doseEraData = List(
        (
          (0, 903963, "", ""),
          List((dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate)))
        )
      )

      val result = doseEra.build.collect

      assert(result.length == 1)
      assert(result(0) == (0, 903963, "", "",
        dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate)))
    }

    it("returns an RDD with two non overlapping ranges") {
      val firstDate = "20080605"
      val secondDate = "20080331"
      doseEraData = List(
        (
          (0, 903963, "", ""),
          List((dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate)))
        ),
        (
          (0, 948078, "", ""),
          List((dateStringFormatter.parseDateTime(secondDate), dateStringFormatter.parseDateTime(secondDate)))
        )
      )

      val result = doseEra.build.collect

      assert(result.length == 2)
      assert(result(0) == (0, 903963, "", "",
        dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate))
      )
      assert(result(1) == (0, 948078, "", "",
        dateStringFormatter.parseDateTime(secondDate), dateStringFormatter.parseDateTime(secondDate))
      )
    }

    it("returns an RDD with two non overlapping ranges yet there are three ranges but two are equal") {
      val firstDate = "20080605"
      val secondDate = "20080331"
      doseEraData = List(
        (
          (0, 903963, "", ""),
          List((dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate)))
        ),
        (
          (0, 948078, "", ""),
          List((dateStringFormatter.parseDateTime(secondDate), dateStringFormatter.parseDateTime(secondDate)))
        ),
        (
          (0, 948078, "", ""),
          List((dateStringFormatter.parseDateTime(secondDate), dateStringFormatter.parseDateTime(secondDate)))
        )
      )

      val result = doseEra.build.collect

      assert(result.length == 2)
      assert(result(0) == (0, 903963, "", "",
        dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate))
      )
      assert(result(1) == (0, 948078, "", "",
        dateStringFormatter.parseDateTime(secondDate), dateStringFormatter.parseDateTime(secondDate))
      )
    }

    it("returns an RDD with two ranges, one overlapping") {
      val firstDate = "20080605"
      val secondDate = "20080331"
      val thirdDate = "20080410"
      doseEraData = List(
        (
          (0, 903963, "", ""),
          List((dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate)))
        ),
        (
          (0, 948078, "", ""),
          List((dateStringFormatter.parseDateTime(secondDate), dateStringFormatter.parseDateTime(secondDate)))
          ),
        (
          (0, 948078, "", ""),
          List((dateStringFormatter.parseDateTime(thirdDate), dateStringFormatter.parseDateTime(thirdDate)))
        )
      )

      val result = doseEra.build.collect

      assert(result.length == 2)
      assert(result(0) == (0, 903963, "", "",
        dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate))
      )
      assert(result(1) == (0, 948078, "", "",
        dateStringFormatter.parseDateTime(secondDate), dateStringFormatter.parseDateTime(thirdDate))
      )
    }

    it("returns an RDD with three ranges, one overlapping") {
      val firstDate = "20080605"
      val secondDate = "20080331"
      val thirdDate = "20080410"
      val fourthDate = "20100901"
      doseEraData = List(
        (
          (0, 903963, "", ""),
          List((dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate)))
        ),
        (
          (0, 903963, "", ""),
          List((dateStringFormatter.parseDateTime(fourthDate), dateStringFormatter.parseDateTime(fourthDate)))
        ),
        (
          (0, 948078, "", ""),
          List((dateStringFormatter.parseDateTime(secondDate), dateStringFormatter.parseDateTime(secondDate)))
        ),
        (
          (0, 948078, "", ""),
          List((dateStringFormatter.parseDateTime(thirdDate), dateStringFormatter.parseDateTime(thirdDate)))
        )
      )

      val result = doseEra.build.collect

      assert(result.length == 3)
      // sort is not random unlike the original one
      assert(result(0) == (0, 903963, "", "",
        dateStringFormatter.parseDateTime(fourthDate), dateStringFormatter.parseDateTime(fourthDate))
      )
      assert(result(1) == (0, 903963, "", "",
        dateStringFormatter.parseDateTime(firstDate), dateStringFormatter.parseDateTime(firstDate))
      )
      assert(result(2) == (0, 948078, "", "",
        dateStringFormatter.parseDateTime(secondDate), dateStringFormatter.parseDateTime(thirdDate))
      )
    }
  }

  describe("writeCSV") {
    it("does nothing considering it has nothing to write") {
      intercept[NullPointerException] {
        doseEra.writeCSV
      }
    }

    it("writes a csv file out") {
      when(conf.getString("ohdsi.csv.location")).thenReturn("/tmp/")

  /*    conditionEra.build
      conditionEra.writeCSV*/
    }

  }
}
package com.github.amchang.ohdsi.lib

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrameReader, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}
import org.mockito.Matchers._
import org.mockito.Mockito._


/**
  * Test the condition era class
  */
class ConditionEraSpec extends RDDSpec with BeforeAndAfter with MockitoSugar with BeforeAndAfterAll {

  // setup values
  implicit var conf: Config = null
  implicit var sparkCont: SparkContext = null
  var sqlCont: SQLContext = null

  var conditionEra: ConditionEra = null
  var conditionOccurrenceData: List[Row] = List()
  val dateStringFormat = "yyyyMMdd"
  var dateStringFormatter: DateTimeFormatter = null

  override protected def beforeAll() = {
    super.beforeAll
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("dose_era_spec")
      .setMaster("local")
    conf = mock[Config]
    sparkCont = new SparkContext(sparkConf)
    sqlCont = new SQLContext(sparkCont)

    dateStringFormatter = DateTimeFormat.forPattern(dateStringFormat)
    when(conf.getString("ohdsi.dateFormat")).thenReturn(dateStringFormat)
  }

  override protected def afterAll() = {
    sparkCont.stop
  }

  before {
    conditionEra = new ConditionEra() {
      override def csvDataReader: DataFrameReader = {
        val dataFrameReader = mock[DataFrameReader]

        val numList: RDD[Row] = sparkCont.parallelize(conditionOccurrenceData)
        val struct = StructType(List(
          StructField("id", StringType, true),
          StructField("person_id", StringType, true),
          StructField("condition_concept_id", StringType, true),
          StructField("condition_start_date", StringType, true),
          StructField("condition_end_date", StringType, true)
        ))
        val dataFrame = sqlCont.createDataFrame(numList, struct)

        // single frame
        when(dataFrameReader.load(anyString())).thenReturn(dataFrame)

        dataFrameReader
      }
    }
  }

  describe("build") {
    it("returns an empty RDD") {
      assert(conditionEra.build.isEmpty)
    }

    it("returns a singular RDD") {
      conditionOccurrenceData = List(
        Row("165361", "0", "138525", "20100630", "20100630")
      )

      val result = conditionEra.build.collect

      assert(result.length == 1)
      assert(result(0) == (0, 138525, dateStringFormatter.parseDateTime("20100630"), dateStringFormatter.parseDateTime("20100630"), 1))
    }

    it("returns an RDD with two non overlapping ranges") {
      conditionOccurrenceData = List(
        Row("165361", "0", "138525", "20100630", "20100630"),
        Row("178861", "0", "138525", "20091203", "20091203")
      )

      val result = conditionEra.build.collect

      assert(result.length == 2)
      assert(result(0) == (0, 138525, dateStringFormatter.parseDateTime("20100630"), dateStringFormatter.parseDateTime("20100630"), 1))
      assert(result(1) == (0, 138525, dateStringFormatter.parseDateTime("20091203"), dateStringFormatter.parseDateTime("20091203"), 1))
    }

    it("returns an RDD with two ranges, one overlapping") {
      conditionOccurrenceData = List(
        Row("165361", "0", "138525", "20100630", "20100630"),
        Row("142861", "0", "138525", "20100609", "20100609"),
        Row("178861", "0", "138525", "20091203", "20091203")
      )

      val result = conditionEra.build.collect

      assert(result.length == 2)
      assert(result(0) == (0, 138525, dateStringFormatter.parseDateTime("20100609"), dateStringFormatter.parseDateTime("20100630"), 2))
      assert(result(1) == (0, 138525, dateStringFormatter.parseDateTime("20091203"), dateStringFormatter.parseDateTime("20091203"), 1))
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
      assert(result(0) == (0, 138525, dateStringFormatter.parseDateTime("20100609"), dateStringFormatter.parseDateTime("20100630"), 2))
      assert(result(1) == (0, 138525, dateStringFormatter.parseDateTime("20091203"), dateStringFormatter.parseDateTime("20091203"), 1))
      assert(result(2) == (1, 138525, dateStringFormatter.parseDateTime("20120104"), dateStringFormatter.parseDateTime("20120201"), 1))
    }
  }

  describe("writeCSV") {
    it("does nothing considering it has nothing to write") {
      assert(conditionEra.writeCSV.isEmpty)
    }

    it("writes a csv file out") {
      import better.files._
      import java.io.{File => JFile}

      conditionOccurrenceData = List(
        Row("165361", "0", "138525", "20100630", "20100630"),
        Row("142861", "0", "138525", "20100609", "20100609"),
        Row("178861", "0", "138525", "20091203", "20091203"),
        Row("208861", "1", "138525", "20120104", "20120201")
      )

      when(conf.getString("ohdsi.csv.location")).thenReturn("/tmp/")

      conditionEra.build

      val result = conditionEra.writeCSV.get
      val resultFile = File(s"${result}/part-00000")

      assert(resultFile.exists)

      val lines = resultFile.lines.toArray
      assert(lines(0) == "condition_occurrence_id,person_id,condition_concept_id,condition_era_start_date,condition_era_end_date,condition_occurrence_count")
      assert(lines(1) == "0,0,138525,20100609,20100630,2")
      assert(lines(2) == "1,0,138525,20091203,20091203,1")
      assert(lines(3) == "2,1,138525,20120104,20120201,1")
    }
  }

}

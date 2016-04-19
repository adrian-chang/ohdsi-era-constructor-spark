package com.github.amchang.ohdsi.lib

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}
import org.mockito.Matchers._
import org.mockito.Mockito._


/**
  * Test the condition era class
  */
class ConditionEraSpec extends FunSpec with BeforeAndAfter with MockitoSugar with BeforeAndAfterAll {

  // setup values
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("drug_exposure_spec")
    .setMaster("local")
  implicit val conf = mock[Config]
  implicit val sparkCont: SparkContext = new SparkContext(sparkConf)
  val sqlCont: SQLContext = new SQLContext(sparkCont)

  var conditionEra: ConditionEra = null

  before {
    conditionEra = new ConditionEra() {
      override def csvDataReader: DataFrameReader = {
        val dataframeReader = mock[DataFrameReader]

        val numList: RDD[Row] = sparkCont.parallelize(List())
        val struct = StructType(List())
        val dataFrame = sqlCont.createDataFrame(numList, struct)

        // single frame
        when(dataframeReader.load(anyString())).thenReturn(dataFrame)

        dataframeReader
      }
    }
  }

  describe("build") {
    it("returns an empty RDD") {

    }
  }

  describe("writeCSV") {

  }



}

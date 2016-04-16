package com.github.amchang.ohdsi.lib

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._


/**
  * Test out the Spark trait
  */
class SparkSpec extends FunSpec with BeforeAndAfter with PrivateMethodTester with MockitoSugar {

  describe("Spark") {

    var sparkCreation: Spark = null
    var conf: Config = null
    var sparkCont: SparkContext = null
    var sqlCont: SQLContext = null

    before {
      sparkCont = mock[SparkContext]
      conf = mock[Config]
      sqlCont = mock[SQLContext]


      sparkCreation = new Spark {
        val config: Config = conf
        val sparkContext: SparkContext = sparkCont
        val sqlContext: SQLContext = sqlCont
      }
    }

    def verifyCsv(privateMethod: PrivateMethod[DataFrameReader]): DataFrameReader = {
      val dataFrameReader = mock[DataFrameReader]

      when(sqlCont.read).thenReturn(dataFrameReader)
      when(dataFrameReader.format("com.databricks.spark.csv")).thenReturn(dataFrameReader)
      when(dataFrameReader.option("header", "true")).thenReturn(dataFrameReader)
      when(dataFrameReader.option("inferSchema", "false")).thenReturn(dataFrameReader)
      when(dataFrameReader.option("mode", "DROPMALFORMED")).thenReturn(dataFrameReader)
      when(dataFrameReader.option("quote", null)).thenReturn(dataFrameReader)
      when(dataFrameReader.option("delimiter", "\t")).thenReturn(dataFrameReader)

      sparkCreation.invokePrivate(privateMethod())

      verify(sqlCont).read
      verify(dataFrameReader).format("com.databricks.spark.csv")
      verify(dataFrameReader).option("header", "true")
      verify(dataFrameReader).option("inferSchema", "false")
      verify(dataFrameReader).option("mode", "DROPMALFORMED")
      verify(dataFrameReader).option("quote", null)

      dataFrameReader
    }

    it("has a spark context available") {

    }

    it("has a SQLContext available") {

    }

    it("has a config available") {
      val privateMethod = PrivateMethod[Config]('config)

    }

    describe("csvVocabReader") {
      it("returns a generic data reader with custom delimiter") {
        val privateMethod = PrivateMethod[DataFrameReader]('csvVocabReader)

        verify(verifyCsv(privateMethod)).option("delimiter", "\t")
      }
    }

    describe("csvDataReader") {
      it("returns a generic data reader with no custom delimiter") {
        val privateMethod = PrivateMethod[DataFrameReader]('csvDataReader)

        verifyCsv(privateMethod)
      }
    }

    describe("getVocabFile") {
      it("returns the file path for a specific vocab file") {
        val privateMethod = PrivateMethod[String]('getVocabFile)
        val cacheLocation = "/usr/"
        val file = "abc.csv"

        when(conf.getString("ohdsi.vocab")).thenReturn(cacheLocation)

        val result = sparkCreation.invokePrivate(privateMethod(file))

        assert(result == "/usr/abc.csv")
      }
    }

    describe("getDataFile") {
      it("returns the file path for a specific piece of data") {
        val privateMethod = PrivateMethod[String]('getDataFile)
        val cacheLocation = "/user/"
        val file = "abc.csv"
        when(conf.getString("ohdsi.data")).thenReturn(cacheLocation)

        val result = sparkCreation.invokePrivate(privateMethod(file))

        assert(result == "/user/abc.csv")
      }
    }

    describe("getCacheFile") {
      it("returns the cache full path") {
        val privateMethod = PrivateMethod[String]('getCacheFile)
        val cacheLocation = "/home"
        val file = "abc"
        when(conf.getString("ohdsi.cache")).thenReturn(cacheLocation)

        val result = sparkCreation.invokePrivate(privateMethod(file))

        assert(result == "/home/cache/abc")
      }
    }

  }

}


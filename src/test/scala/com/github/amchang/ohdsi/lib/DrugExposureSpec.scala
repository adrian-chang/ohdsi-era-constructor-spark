package com.github.amchang.ohdsi.lib

import java.lang.reflect.Field

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar


/**
  * Test out DrugExposure
  */
class DrugExposureSpec extends RDDSpec with BeforeAndAfter with MockitoSugar with BeforeAndAfterAll {

  // entire base data
  var conf: Config = null
  var sparkCont: SparkContext = null
  var sqlCont: SQLContext = null
  // stub the cache file location
  val dateStringFormat = "yyyyMMdd"
  var formatter: DateTimeFormatter = null

  // schemas
  val drugExposureSchema = StructType(List(
    StructField("id", StringType, true),
    StructField("person_id", StringType, true),
    StructField("drug_concept_id", StringType, true),
    StructField("exposure_start_date", StringType, true),
    StructField("exposure_end_date", StringType, true), // 4
    StructField("foo", StringType, true),
    StructField("bar", StringType, true),
    StructField("foo1", StringType, true),
    StructField("bar1", StringType, true), // 8
    StructField("days_supply", StringType, true),
    StructField("bar2", StringType, true),
    StructField("foo3", StringType, true),
    StructField("dose_value", StringType, true),
    StructField("unit_concept_id", StringType, true) // 13
  ))

  val conceptAncestorSchema = StructType(List(
    StructField("ancestor_concept_id", StringType, true),
    StructField("descendant_concept_id", StringType, true)
  ))

  val conceptSchema = StructType(List(
    StructField("id", StringType, true),
    StructField("foo", StringType, true),
    StructField("bar", StringType, true),
    StructField("vocabulary_id", StringType, true),
    StructField("concept_class", StringType, true)
  ))

  var drugExposure: DrugExposure = null
  var loadDrugExposure: Field = null
  var loadConceptAncestor: Field = null
  var loadConcept: Field = null
  var createInitialData: () => RDD[((Int, Int, String, String), List[(DateTime, DateTime)])] = null

  var exposureData: List[Row] = List()
  var conceptAncestorData: List[Row] = List()
  var conceptData: List[Row] = List()

  before {
    drugExposure = new DrugExposure {
      val config: Config = conf
      val sparkContext: SparkContext = sparkCont
      val sqlContext: SQLContext = sqlCont
    }

    val drugExposureClass = classOf[DrugExposure]
    val allFields: Array[Field] = drugExposureClass.getDeclaredFields

    val createInitData: Field = allFields(0)
    createInitData.setAccessible(true)
    createInitialData = createInitData.get(drugExposure).asInstanceOf[() => RDD[((Int, Int, String, String), List[(DateTime, DateTime)])]]

    // setup all of the initial data here
    loadDrugExposure = allFields(2)
    loadDrugExposure.setAccessible(true)

    loadConceptAncestor = allFields(3)
    loadConceptAncestor.setAccessible(true)

    loadConcept = allFields(4)
    loadConcept.setAccessible(true)

    loadDrugExposure.set(drugExposure, () => {
      val numList: RDD[Row] = sparkCont.parallelize(exposureData)

      sqlCont.createDataFrame(numList, drugExposureSchema)
    }: DataFrame)

    loadConceptAncestor.set(drugExposure, () => {
      val numList: RDD[Row] = sparkCont.parallelize(conceptAncestorData)

      sqlCont.createDataFrame(numList, conceptAncestorSchema)
    }: DataFrame)

    loadConcept.set(drugExposure, () => {
      val numList: RDD[Row] = sparkCont.parallelize(conceptData)

      sqlCont.createDataFrame(numList, conceptSchema)
    }: DataFrame)
  }

  override protected def beforeAll() = {
    super.beforeAll
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("drug_exposure_spec")
      .setMaster("local")
    conf = mock[Config]
    sparkCont = new SparkContext(sparkConf)
    sqlCont = new SQLContext(sparkCont)

    formatter = DateTimeFormat.forPattern(dateStringFormat)
    when(conf.getString("ohdsi.dateFormat")).thenReturn(dateStringFormat)
    when(conf.getString("ohdsi.cache.location")).thenReturn("/tmp")
    when(conf.getString("ohdsi.dateFormat")).thenReturn(dateStringFormat)
  }

  override protected def afterAll() = {
    sparkCont.stop
  }

  describe("DrugExposure") {

    describe("createInitialData") {


      it("returns an empty rdd since drug exposure is empty") {
        assert(createInitialData().count == 0)
      }

      it("returns an empty rdd since no concept or concept ancestor ids could be found") {
        exposureData = List(
          Row("1", "10", "20", "20200419", "20200421", "", "", "", "", "", "", "", "100", "30"),
          Row("2", "11", "21", "20200413", "", "", "", "", "", "20", "", "", "200", "30")
        )

        assert(createInitialData().count == 0)
      }

      it("returns an empty rdd since no concept ancestor ids count be found") {
        conceptData = List(
          Row("89", "", "", "RxNorm", "Ingredient"),
          Row("103", "", "", "RxNorm", "Ingredient"),
          Row("120", "", "", "RxNorm", "Ingredient"),
          Row("23", "", "", "Diag", "Screening")
        )

        assert(createInitialData().count == 0)
      }

      it("returns an empty rdd since no concept ids could be found") {
        conceptData = List()
        conceptAncestorData = List(
          Row("103", "9012"),
          Row("120", "890")
        )

        assert(createInitialData().count == 0)
      }

      it("returns an empty rdd since no descendant concept ids are used in drug exposure records") {
        exposureData = List(
          Row("1", "10", "6", "20200419", "20200421", "", "", "", "", "", "", "", "100", "30"),
          Row("2", "11", "5", "20200413", "", "", "", "", "", "20", "", "", "200", "30"),
          Row("3", "11", "21", "20200413", "", "", "", "", "", "20", "", "", "200", "30")
        )

        conceptAncestorData = List(
          Row("103", "9012"),
          Row("120", "890")
        )

        conceptData = List(
          Row("89", "", "", "RxNorm", "Ingredient"),
          Row("103", "", "", "RxNorm", "Ingredient"),
          Row("120", "", "", "RxNorm", "Ingredient"),
          Row("23", "", "", "Diag", "Screening")
        )

        assert(createInitialData().count == 0)
      }

      it("returns an rdd mapping since concept ids / ancestor ids / drug exposure records all line up") {
        exposureData = List(
          Row("1", "10", "9012", "20200419", "20200421", "", "", "", "", "", "", "", "100", "30"),
          Row("2", "11", "890", "20200413", "", "", "", "", "", "20", "", "", "200", "30"),
          Row("3", "11", "21", "20200413", "", "", "", "", "", "20", "", "", "200", "30")
        )

        conceptAncestorData = List(
          Row("103", "9012"),
          Row("120", "890")
        )

        conceptData = List(
          Row("89", "", "", "RxNorm", "Ingredient"),
          Row("103", "", "", "RxNorm", "Ingredient"),
          Row("120", "", "", "RxNorm", "Ingredient"),
          Row("23", "", "", "Diag", "Screening")
        )

        val result = createInitialData().collect
        val firstRecord = result(0)
        val secondRecord = result(1)

        assert(result.length == 2)
        assert(firstRecord == ((10,103,"30","100"), List((formatter.parseDateTime("20200419"), formatter.parseDateTime("20200421")))))
        assert(secondRecord == ((11,120,"30","200"),List((formatter.parseDateTime("20200413"), formatter.parseDateTime("20200503")))))
      }

      it("returns the same version as its cached now") {
        exposureData = List(
          Row("1", "10", "9012", "20200419", "20200421", "", "", "", "", "", "", "", "100", "30"),
          Row("2", "11", "890", "20200413", "", "", "", "", "", "20", "", "", "200", "30"),
          Row("3", "11", "21", "20200413", "", "", "", "", "", "20", "", "", "200", "30")
        )

        conceptAncestorData = List()
        conceptData = List()

        when(conf.getBoolean("ohdsi.cache.enabled")).thenReturn(true)

        val result = createInitialData().collect
        val firstRecord = result(0)
        val secondRecord = result(1)

        assert(result.length == 2)
        assert(firstRecord == ((10, 103, "30", "100"), List((formatter.parseDateTime("20200419"), formatter.parseDateTime("20200421")))))
        assert(secondRecord == ((11, 120, "30", "200"), List((formatter.parseDateTime("20200413"), formatter.parseDateTime("20200503")))))
      }
    }
  }
}

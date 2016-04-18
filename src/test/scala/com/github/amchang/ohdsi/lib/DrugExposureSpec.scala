package com.github.amchang.ohdsi.lib

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._


/**
  * Test out DrugExposure
  */
class DrugExposureSpec extends FunSpec with BeforeAndAfter with MockitoSugar {

  var drugExposure: DrugExposure = null
  var conf: Config = null
  var sparkCont: SparkContext = null
  var sqlCont: SQLContext = null

  before {
    sparkCont = mock[SparkContext]
    conf = mock[Config]
    sqlCont = mock[SQLContext]


    drugExposure = new DrugExposure {
      val config: Config = conf
      val sparkContext: SparkContext = sparkCont
      val sqlContext: SQLContext = sqlCont
    }
  }

  describe("DrugExposure") {

    it("has several types defined") {
      //assert(DrugExposure.)
    }

  }

}

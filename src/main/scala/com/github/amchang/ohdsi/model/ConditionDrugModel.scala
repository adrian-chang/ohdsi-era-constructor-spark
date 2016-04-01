package com.github.amchang.ohdsi.model
import scala.concurrent.Future

/**
  * Base condition drug model
  */
object ConditionDrugModel extends Model {

  /**
    * Get the count of people with a condition occurrence and drug exposure
    * @return
    */
  def stats: Future[String] = {
    Future {
      ""
    }
  }
}

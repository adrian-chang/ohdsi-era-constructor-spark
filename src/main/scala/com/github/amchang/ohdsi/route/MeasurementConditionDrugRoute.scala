package com.github.amchang.ohdsi.route

import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.protocols.http.{HttpRequest, HttpResponse}
import colossus.service.Callback
import Callback.Implicits._

/**
  * Handle iris_obs_dx_rx queries
  */
object MeasurementConditionDrugRoute extends Route {

  /**
    * Handle /measurementConditionDrug
    * @return a partial function to handle routes
    */
  def route: PartialFunction[HttpRequest, Callback[HttpResponse]] = {
    case req @ Get on Root / "person" =>
      req.ok("fo1o").withHeader("Content-Type", "application/json")
  }

}

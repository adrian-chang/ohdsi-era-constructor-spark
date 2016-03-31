package com.github.amchang.ohdsi.route

import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpRequest, HttpResponse, UrlParsing}
import colossus.service.Callback
import Callback.Implicits._
import UrlParsing._
import colossus.core.WorkerRef

/**
  * Handle iris_dx_rx
  */
object ConditionDrugRoute extends Route {

  /**
    * Handle /conditionDrug routes
 *
    * @return a partial function to handle routes
    */
  def route(worker: WorkerRef): PartialFunction[HttpRequest, Callback[HttpResponse]] = {
    case req @ Get on Root / "conditionDrug" =>
      req.ok("foo")
  }
}

package com.github.amchang.ohdsi.route

import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.protocols.http.{HttpRequest, HttpResponse}
import colossus.service.Callback
import Callback.Implicits._

/**
  * Handle iris_dx_proc routes
  */
object ConditionProcedureRoute extends Route {

  /**
    * Handle /conditionProcedure routes
    * @return a partial function to handle routes
    */
  def route: PartialFunction[HttpRequest, Callback[HttpResponse]] = {
    case req @ Get on Root / "conditionProcedure" =>
      req.ok("foo")
  }
}

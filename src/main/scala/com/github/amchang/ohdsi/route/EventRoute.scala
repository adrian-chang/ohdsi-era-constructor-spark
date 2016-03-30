package com.github.amchang.ohdsi.route

import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpRequest, HttpResponse, UrlParsing}
import colossus.service.Callback
import Callback.Implicits._
import UrlParsing._

/**
  * Handle queries related to an iris_event
  */
object EventRoute {

  /**
    * Handle /person routes
    *
    * @return a partial function to handle routes
    */
  def route: PartialFunction[HttpRequest, Callback[HttpResponse]] = {
    case req @ Get on Root / "event" =>
      req.ok("foo")
  }

}

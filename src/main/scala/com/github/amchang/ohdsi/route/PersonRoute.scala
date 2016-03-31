package com.github.amchang.ohdsi.route

import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpRequest, HttpResponse, UrlParsing}
import colossus.service.Callback
import Callback.Implicits._
import UrlParsing._
import com.github.amchang.ohdsi.model.PersonModel


/**
  * Handle queries related to a iris_person
  */
object PersonRoute extends Route {

  /**
    * Handle /person routes
 *
    * @return a partial function to handle routes
    */
  def route: PartialFunction[HttpRequest, Callback[HttpResponse]] = {
    case req @ Get on Root / "person" =>
      PersonModel.process
      req.ok("fo1o").withHeader("Content-Type", "application/json")
  }

}

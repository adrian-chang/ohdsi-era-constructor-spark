package com.github.amchang.ohdsi.route
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.protocols.http.{HttpRequest, HttpResponse}
import colossus.service.Callback
import Callback.Implicits._
import colossus.core.WorkerRef


/**
  * Handle queries related to dead
  */
object DeadRoute extends Route {

  /**
    * Handle /dead routes
 *
    * @return a partial function to handle routes
    */
  def route(worker: WorkerRef): PartialFunction[HttpRequest, Callback[HttpResponse]] = {
    case req @ Get on Root / "person" =>
      req.ok("1234")
  }

}

package com.github.amchang.ohdsi.route
import colossus.core.WorkerRef
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.protocols.http.{HttpRequest, HttpResponse}
import colossus.service.Callback
import Callback.Implicits._

/**
  * Health check for the system
  */
object HealthRoute extends Route {

  /**
    * Define the /health route
    *
    * @return a partial function to handle routes
    */
  def route(worker: WorkerRef): PartialFunction[HttpRequest, Callback[HttpResponse]] = {
    case req @ Get on Root =>
      req.ok("up")
  }
}

package com.github.amchang.ohdsi.route

import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpRequest, HttpResponse, UrlParsing}
import colossus.service.Callback
import UrlParsing._
import colossus.core.WorkerRef
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
  def route(workerRef: WorkerRef): PartialFunction[HttpRequest, Callback[HttpResponse]] = {
    // root person action
    case req @ Get on Root / "person" =>
      implicit val executor = workerRef.callbackExecutor
      Callback.fromFuture(PersonModel.stats).map { result =>
        req.ok(result.get)
          .withHeader("Content-Type", "application/json")
      }
  }

}

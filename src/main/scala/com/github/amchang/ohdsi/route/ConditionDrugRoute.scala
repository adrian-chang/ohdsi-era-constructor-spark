package com.github.amchang.ohdsi.route

import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.{Root, on}
import colossus.protocols.http.{HttpRequest, HttpResponse, UrlParsing}
import colossus.service.Callback
import Callback.Implicits._
import UrlParsing._
import colossus.core.WorkerRef
import com.github.amchang.ohdsi.model.ConditionDrugModel

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
    case req@Get on Root / "conditionDrug" =>
      implicit val executor = worker.callbackExecutor
      Callback.fromFuture(ConditionDrugModel.stats).map { result =>
        req.ok(result)
          .withHeader("Content-Type", "application/json")
      }
  }

}

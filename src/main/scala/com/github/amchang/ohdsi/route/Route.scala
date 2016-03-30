package com.github.amchang.ohdsi.route

import colossus.protocols.http.{HttpRequest, HttpResponse}
import colossus.service.Callback

/**
  * All routes must have a partial route function
  */
trait Route {

  /**
    * Define this route handler
    * @return a partial function to handle routes
    */
  def route: PartialFunction[HttpRequest, Callback[HttpResponse]]

}

package com.github.amchang.ohdsi.service

import colossus.IOSystem
import colossus.core._
import colossus.protocols.http.{Http, HttpRequest, HttpResponse, HttpService, UrlParsing}
import colossus.service.{Callback, ServiceConfig}

import com.github.amchang.ohdsi.route.{ConditionDrugRoute, EventRoute, PersonRoute}


/**
  * This is a service just to do the person calculations
  */
object HttpService {

  // the name of our service
  private val name = "http-service"

  /**
    * The actual person service handler
    * @param context the current connection context
    */
  class Service(context: ServerContext) extends HttpService(ServiceConfig(), context) {

    /**
      * Handle all of the routes here
      * @return handle to handle all routes
      */
    def handle  =
      PersonRoute.route orElse
      EventRoute.route orElse
      ConditionDrugRoute.route
  }

  /**
    * Start the person service
    *
    * @param port port the start the server in
    * @param io current io system
    * @return Reference to the new server
    */
  def start(port: Int)(implicit io: IOSystem): ServerRef = {
    Server.start(name, port) { worker =>
      new Initializer(worker) {
        def onConnect = (context) => new Service(context)
      }
    }
  }

}

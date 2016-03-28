package com.github.amchang.ohdsi.service

import colossus.IOSystem
import colossus.core._
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.UrlParsing.Root
import colossus.protocols.http.{Http, HttpService}
import colossus.service.ServiceConfig


/**
  * This is a service just to do the person calculations
  */
object PersonService {

  class Service(context: ServerContext) extends HttpService(ServiceConfig(), context) {
    def handle = {
      case req @ Get on Root => req.ok("Hello World!")
    }
  }


  /**
    * Start the person service
    *
    * @param port port the start the server in
    * @param io current io system
    * @return Reference to the new server
    */
  def start(port: Int)(implicit io: IOSystem): ServerRef = {
    Server.start("person-service", port) { worker =>
      new Initializer(worker) {
        def onConnect = (context) => new Service(context)
      }
    }
  }

}

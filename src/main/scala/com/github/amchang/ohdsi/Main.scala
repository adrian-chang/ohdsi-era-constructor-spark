package com.github.amchang.ohdsi

import akka.actor.ActorSystem
import colossus.IOSystem
import com.github.amchang.ohdsi.service.HttpService
import com.typesafe.config.ConfigFactory

/*
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.util.URLParser
import com.github.mauricio.async.db.util.ExecutorServiceUtils.CachedExecutionContext
import com.github.mauricio.async.db.{RowData, QueryResult, Connection}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}*/

/**
  * Start point for the entire program
  */
object Main {

  def main(args: Array[String]) {
    // set up colossus
    implicit val actorSystem = ActorSystem("ohdsi")
    implicit val io = IOSystem()

    val config = ConfigFactory.load()


    // just for people queries
    HttpService.start(9001)
   /* val configuration = URLParser.parse("jdbc:postgresql://database:5432/ohdsi?user=ohdsi")
    val connection: Connection = new PostgreSQLConnection(configuration)

    Await.result(connection.connect, 5 seconds)

    val future: Future[QueryResult] = connection.sendQuery("SELECT 0")

    val mapResult: Future[Any] = future.map(queryResult => queryResult.rows match {
      case Some(resultSet) => {
        val row : RowData = resultSet.head
        row(0)
      }
      case None => -1
    }
    )

    val result = Await.result( mapResult, 5 seconds )

    println(result)

    connection.disconnect*/
  }

}
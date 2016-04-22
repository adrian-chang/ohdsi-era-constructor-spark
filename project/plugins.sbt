resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

// big jar
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.2")
// docker
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.3.0")
addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.0.6-M2")




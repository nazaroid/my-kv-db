logLevel := Level.Info

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
addSbtPlugin("com.typesafe.sbt" %% "sbt-native-packager" % "1.8.1")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.13.0")
addDependencyTreePlugin

libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.9.7"
)


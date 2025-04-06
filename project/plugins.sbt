logLevel := Level.Info

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("com.typesafe.sbt" %% "sbt-native-packager" % "1.8.1")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.18")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.13.0")
addDependencyTreePlugin

libraryDependencies ++= Seq(
  "org.eclipse.jgit" % "org.eclipse.jgit" % "5.3.0.201903130848-r",
  "com.thesamet.scalapb" %% "compilerplugin" % "0.9.7"
)

Compile / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "project" / "git" / "plugin"


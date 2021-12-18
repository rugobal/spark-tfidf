import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.rugobal",
      scalaVersion := "2.12.14",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "tfidf-spark",
    libraryDependencies ++= all
  )

resolvers += Resolver.mavenLocal
import sbt._

object Version {
  val spark = "3.0.0"
  val scalaTest = "3.2.10"
}

object Library {

  // general
  lazy val scalaTest = "org.scalatest" %% "scalatest" % Version.scalaTest

  // spark
  lazy val sparkCore         = "org.apache.spark" %% "spark-core" % Version.spark
  lazy val sparkSql         = "org.apache.spark" %% "spark-sql" % Version.spark

  // other

}

object Dependencies {
  import Library._

  val common = Seq(
    scalaTest % Test withSources() withJavadoc()
  )

  val spark = Seq(
    sparkCore           % "provided" withSources() withJavadoc(),
    sparkSql            % "provided" withSources() withJavadoc()
  )

  val other = Seq(

  )

  val all = common ++ spark ++ other
}

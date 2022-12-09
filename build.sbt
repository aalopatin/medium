ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.16"

lazy val sparkVersion = "3.2.1"

//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", _*) => MergeStrategy.discard
//  case _                        => MergeStrategy.first
//}

lazy val commonSettingsSpark = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion
    , "org.apache.spark" %% "spark-sql" % sparkVersion
  ).map(_ % "provided")
)

lazy val root = (project in file("."))
  .settings(
    name := "medium"
  )

lazy val flatteningStructuredData = (project in file("flattening-structured-data"))
  .settings(
    name := "flattening-structured-data",
    commonSettingsSpark
  )

lazy val tailrecDynamicSources = (project in file("tailrec-dynamic-sources"))
  .settings(
    name := "tailrec-dynamic-sources",
    commonSettingsSpark,
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.4.2"
      , "org.postgresql" % "postgresql" % "42.3.6"
    )
  )

lazy val fillMissingDates = (project in file("fill-missing-dates"))
  .settings(
    name := "fill-missing-dates",
    commonSettingsSpark
  )

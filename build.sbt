
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "exercise",
      scalaVersion := "2.11.8",
      version      := "0.1.0"
    )),
    name := "Inverted Index",
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.1" % Test,
    "org.apache.spark" %% "spark-core" % "2.1.1",
    "org.apache.spark" %% "spark-sql" % "2.1.0"
    )
  )

lazy val root = (project in file("."))
  .settings(
    name         := "COVID-19",
    scalaVersion := "2.12.10",
    version      := "0.1.0-SNAPSHOT",
    libraryDependencies += "org.scalafx" % "scalafx_2.11" % "8.0.102-R11",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.5",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.5"
  )

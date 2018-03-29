name := "Spark Hello World"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq( 
    "org.apache.spark" %% "spark-sql" % "2.2.0",
    "log4j" % "log4j" % "1.2.17" 
) 

name := "spark_sbt_project"
version := "1.0"
scalaVersion := "2.11.8"
val sparkVersion = "2.0.2"

unmanagedBase := baseDirectory.value / "custom_lib"
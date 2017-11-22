name := "Pipeline"

version := "0.0.1-SNAPSHOT"

assemblyJarName in assembly := "Pipeline.jar"

scalaVersion := "2.11.8"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-shapefile" % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-proj4" % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-vector" % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-s3" % "1.2.0-SNAPSHOT",
  "com.redhat.et" %% "silex" % "0.1.1",
  "com.github.seratch" %% "awscala" % "0.6.+",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test",
  "org.scalactic" %% "scalactic" % "3.0.1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

resolvers ++= Seq[Resolver](
  "LocationTech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots",
  "LocationTech GeoTrellis Releases" at "https://repo.locationtech.org/content/repositories/releases",
  "Geotools" at "http://download.osgeo.org/webdav/geotools/",
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "Will's bintray" at "https://dl.bintray.com/willb/maven/"
)

assemblyMergeStrategy in assembly := {
  case PathList("com", "vividsolutions", xs @ _*) => MergeStrategy.first
  case PathList("com", "fasterxml", "jackson", "databind", xs @ _*) => MergeStrategy.deduplicate
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

name := "Pipeline"

version := "0.0.1-SNAPSHOT"

assemblyJarName in assembly := "Pipeline.jar"

scalaVersion := "2.11.8"

mainClass := Some("edu.upf.inequality.pipeline.Pipeline")

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-shapefile" % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-proj4" % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-vector" % "1.0.0",
  ("org.locationtech.geotrellis" %% "geotrellis-spark" % "1.0.0").
    exclude("org.locationtech", "geotrellis-spark"),
  ("org.locationtech.geotrellis" %% "geotrellis-spark-etl" % "1.0.0").
    exclude("org.locationtech", "geotrellis-spark"),
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
)


resolvers ++= Seq[Resolver](
  "LocationTech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots",
  "Geotools" at "http://download.osgeo.org/webdav/geotools/",
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

assemblyMergeStrategy in assembly := {
  case PathList("com", "vividsolutions", xs @ _*) => MergeStrategy.first
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

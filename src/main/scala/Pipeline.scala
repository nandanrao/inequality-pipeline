package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io._
import geotrellis.shapefile.ShapeFileReader
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._

import GroupByShape._

object Pipeline {
  def main(args: Array[String]) {

    if (args.length != 4) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <tilePath>
        |  <output>
        |  <basicString>
        |  <meanString>
        """.stripMargin)
      System.exit(1)
    }
    // val Array(tilePath, output, basicString, meanString) = args
    // val names = basicString.split(",")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Pipeline")
      .getOrCreate()

    implicit val isc : SparkContext = spark.sparkContext

    // switch for S3!
    // Read from the database created by ETL process (Hadoop FS)
    def makeRDD(layerName: String, path: String, num: Int) = {
      val inLayerId = LayerId(layerName, num)
      require(HadoopAttributeStore(path).layerExists(inLayerId))

      HadoopLayerReader(path)
        .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](inLayerId)
        .result
    }

    // // We use this to query the ETL DB, avoids reading tiles that we won't use.
    // val tilePath = "/Users/nandanrao/Documents/BGSE/inequality/pipeline/etl/tiles"
    // val path = "./municipalities/2006/Election2006_Municipalities.shp"
    // val countries : Seq[MultiPolygonFeature[Map[String, AnyRef]]] = ShapeFileReader.readMultiPolygonFeatures(path)
    // val nl = makeRDD("nl_2013", tilePath, 0)
    // val munis = makeRDD("municipalities", tilePath, 0)
    // val countriesRDD = sc.parallelize(countries.take(30))
    // val crdd = shapeToContextRDD(getId(countriesRDD, "MUNICID"), nl.metadata)
    // writeTiff(crdd, "groupbyshape2.tif")
    // countries.take(10).map(t => shapeToTile(t.geom, 1000, 1000, CRS.fromEpsgCode(4326))).map{ case (e, t) => Raster(t, e.extent)}.take(1)(0)

    // Performs efficient spatial join of two RDDs via SpatialKey
    // Concats values (which should be seq's).
    def joinSingles(
      first: ContextRDD[SpatialKey, Seq[Tile], TileLayerMetadata[SpatialKey]],
      second: ContextRDD[SpatialKey, Seq[Tile], TileLayerMetadata[SpatialKey]]) :
        ContextRDD[SpatialKey, Seq[Tile], TileLayerMetadata[SpatialKey]] = {

      val md = first.metadata
      first.spatialJoin(second)
        .withContext{ _.mapValues{ case (t1, t2) => t1 ++ t2 }}
        .mapContext { bounds => md.updateBounds(bounds) }
    }

    // Seq of RDD's
    // def read(names: Seq[String]) = names.map(makeRDD(_, tilePath))

    // read all normalized raster files from ETL Raster DB into RDDs
    // val reduced = read(names)
    //   .map(rdd => rdd.withContext{ _.mapValues(Seq(_))}) // Seq enables concatting together
    //   .reduce(joinSingles)

    // def writeTiff(r: Raster[Tile], crs: CRS, f: String) : Unit = {
    //   GeoTiff(r, crs).write(f)
    // }

    // def writeTiff(
    //   rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    //   f: String) : Unit = {
    //   GeoTiff(rdd.stitch.crop(rdd.metadata.extent), rdd.metadata.crs).write(f)
    // }
  }
}


package edu.upf.inequality.pipeline

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import geotrellis.spark._

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io._
import geotrellis.spark.tiling.LayoutDefinition

import geotrellis.raster.io.geotiff._
import geotrellis.shapefile.ShapeFileReader

import java.io.File


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
    val Array(tilePath, output, basicString, meanString) = args
    val names = basicString.split(",")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Pipeline")
      .getOrCreate()

    implicit val sc : SparkContext = spark.sparkContext

    // We use this to query the ETL DB, avoids reading tiles that we won't use.
    val path = "foo"
    val countries : Seq[MultiPolygonFeature[Map[String, AnyRef]]] = ShapeFileReader.readMultiPolygonFeatures(path)

    // switch for S3!
    // Read from the database created by ETL process (Hadoop FS)
    def makeRDD(layerName: String, path: String) = {
      val inLayerId = LayerId(layerName, 8)
      require(HadoopAttributeStore(path).layerExists(inLayerId))

      HadoopLayerReader(path)
        .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](inLayerId)
        .result
    }

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
    def read(names: Seq[String]) = names.map(makeRDD(_, tilePath))

    // read all normalized raster files from ETL Raster DB into RDDs
    val reduced = read(names)
      .map(rdd => rdd.withContext{ _.mapValues(Seq(_))}) // Seq enables concatting together
      .reduce(joinSingles)
  }
}

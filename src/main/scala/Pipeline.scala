package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark.io.s3._
import geotrellis.spark.io._
import geotrellis.shapefile.ShapeFileReader

import geotrellis.proj4._

import GroupByShape._
import Gini._
import Implicits._
import Wealth._

object Pipeline {
  def main(args: Array[String]) {

    if (args.length != 7) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <tilePath> path to tiles
        |  <shapePath> path to ShapeFile for aggregations
        |  <nlCode> code of desired nightlight in ETL database
        |  <popCode> code of desired population in ETL database
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }
    val Array(tilePath, shapePath, nlCode, popCode, crush, topCode, outFile) = args
    // val Array(tilePath, shapePath, nlCode, popCode) = Array("/Users/nandanrao/Documents/BGSE/inequality/pipeline/etl/tiles", "./municipalities/2006/Election2006_Municipalities.shp", "nl_2013", "pop_2015")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Pipeline")
      .getOrCreate()

    implicit val sc : SparkContext = spark.sparkContext

    // Read from the database created by ETL process (S3 FS)
    // create a version for hadoop local! 
    def makeRDD(layerName: String, path: String, num: Int) = {
      val inLayerId = LayerId(layerName, num)
      val store = S3AttributeStore(path)
      require(store.layerExists(inLayerId))

      S3LayerReader(store)
        .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](inLayerId)
        .result
    }

    // // We use this to query the ETL DB, avoids reading tiles that we won't use.
    // val countries : Seq[MultiPolygonFeature[Map[String, AnyRef]]] = ShapeFileReader.readMultiPolygonFeatures(shapePath)
    val nl = makeRDD(nlCode, tilePath, 8)
    val pop = makeRDD(popCode, tilePath, 8)

    printRaster(nl, pop, crush.toDouble, topCode.toDouble, outFile)
    // val countriesRDD = getId(sc.parallelize(countries.take(20)), "MUNICID")

  }
}

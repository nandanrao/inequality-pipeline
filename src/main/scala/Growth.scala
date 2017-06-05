package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import geotrellis.spark._
import geotrellis.raster._
import org.apache.spark.sql.{Dataset, SparkSession}
import geotrellis.vector._

// import edu.upf.inequality.pipeline.GroupByShape._
// import edu.upf.inequality.pipeline.IO._
// import edu.upf.inequality.pipeline.Growth._
// import edu.upf.inequality.pipeline.Wealth._

import GroupByShape._
import IO._
import Wealth._


object Growth {
  def main(args: Array[String]) {

    if (args.length != 11) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <tilePath> path to tiles
        |  <shapeKey> path to ShapeFile for aggregations
        |  <shapeId> Id in shapefile ("false" if we are using raster data?)
        |  <maxTileSize> max tile size
        |  <nlKeyA> key of nl A
        |  <nlKeyB> key of nl B
        |  <popKey> code of desired population in ETL database
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }
    val Array(tilePath, shapeKey, shapeId, maxTileSize, nlKeyA, nlKeyB, popKey, crush, topCode, outFile) = args
    // val Array(tilePath, shapeKey, shapeId, maxTileSize, nlKeyA, nlKeyB, popKey, crush, topCode, outFile) = Array("upf-inequality-raw-geotifs", "municipalities/2006/Election2006_Municipalities.shp", "MUNICID", "16384", "tiny-rasters/2012-tiny.tif", "tiny-rasters/2013-tiny.tif", "tiny-rasters/pop-2013-tiny.tif", "4", "99999999", "growth-out-2")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Pipeline")
      .getOrCreate()
    implicit val sc : SparkContext = spark.sparkContext

    val tileSize = maxTileSize.toInt

    val pop = readRDD(tilePath, popKey, tileSize)

    val Seq(wA, wB) = Seq(nlKeyA, nlKeyB)
      .map(readRDD(tilePath, _, tileSize))
      .map(wealthRaster(_, pop, crush.toFloat, topCode.toFloat))

    val shapes = if (shapeId != "false") readShapeFile(tilePath, shapeKey, shapeId, wA.metadata) else readRDD(tilePath, shapeKey, tileSize)

    growth(wA, wB, shapes).coalesce(1).write.csv(outFile)
  }

  // weighted growth is \sum \frac{y^2_{nrt}}{y_nrt-1}
  def weightedGrowth(years: RDD[(Float, Float)]) : Double = {
    years.map{ case (a,b) => b*b / a  }.sum
  }

  case class DI(growth:Double, index: Long)
  case class GrowthRates(code: Int, growth:Double)

  // create covariance???
  // convert to dataset and use .stat.cov() ??
  def cov(rdd: RDD[(Double, Long)])(implicit spark: SparkSession) : Double = {
    import spark.implicits._
    rdd.map{ case (d,i) => DI(d,i) }.toDF.stat.cov("growth", "index")
  }

  // Takes a simple RDD of one years nl/cap and the prev
  def growth(years: RDD[(Float, Float)])(implicit spark: SparkSession) : Double = {

    // Filter years that are NaN
    val y = years.filter{ case (a,b) => !a.isNaN && !b.isNaN }

    // Calcs
    val tg = y.map(_._2).sum
    val wg = weightedGrowth(y)
    val inclusiveGrowth = y.map{ case (a, b) => b/wg - a/tg }
    cov(inclusiveGrowth.zipWithIndex)
  }

  def growth[G <: Geometry](
    wealthA: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    wealthB: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    shapes: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]
  )(implicit spark: SparkSession) : Dataset[GrowthRates] = {
    import spark.implicits._
    val Seq(a,b) = Seq(wealthA, wealthB).map(groupByRasterShapes(shapes, _))

    // Both are products of leftOuterJoin on shapes, so we zip together to
    // combine the keys. 
    val l = a.zip(b).map{ case ((k, v1), (_, v2)) => (k, (v1(0), v2(0)))}

    val keys = a.keys.distinct.collect.toList // take first x to limit? 
    val growthRates = keys.map(k => growth(l.filter(_._1 == k).values))
    spark.sparkContext.parallelize(keys.zip(growthRates)).map{ case (a,b) => GrowthRates(a,b)}.toDS
  }
}

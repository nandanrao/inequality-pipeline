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

    if (args.length != 12) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <tilePath> path to tiles
        |  <shapeKey> path to ShapeFile for aggregations
        |  <shapeId> Id in shapefile ("false" if we are using raster data?)
        |  <maxTileSize> max tile size
        |  <layoutSize> size of floatingLayoutScheme
        |  <numPartitions> partitions for RDD Raster reading
        |  <nlKeyA> key of nl A
        |  <nlKeyB> key of nl B
        |  <popKey> code of desired population in ETL database
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }
    val Array(tilePath, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKeyA, nlKeyB, popKey, crush, topCode, outFile) = args

    // val Array(tilePath, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKeyA, nlKeyB, popKey, crush, topCode, outFile) = Array("upf-inequality-raw-geotifs", "simple/countries-europe.tif", "false", "256", "512", "1024", "simple/nl-2012-europe.tif", "simple/nl-2013-europe.tif", "simple/pop-2013-europe.tif", "4", "99999999", "s3://upf-inequality-raw-geotifs/growth-calcs-6-7")


    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Growth")
      .getOrCreate()
    implicit val sc : SparkContext = spark.sparkContext

    val Seq(tileSize, layout, partitions) = Seq(maxTileSize, layoutSize, numPartitions).map(_.toInt)

    val pop = readRDD(tilePath, popKey, tileSize, layout, partitions)

    val Seq(wA, wB) = Seq(nlKeyA, nlKeyB)
      .map(s => readRDD(tilePath, s, tileSize, layout, partitions))
      .map(wealthRaster(_, pop, crush.toFloat, topCode.toFloat)) // use two pops for wealths!!

    val shapes = if (shapeId != "false") readShapeFile(tilePath, shapeKey, shapeId, wA.metadata) else readRDD(tilePath, shapeKey, tileSize, layout, partitions)

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
    val y = years
      .filter{ case (a,b) => !a.isNaN && !b.isNaN }
      .sortBy(_._2) // sort by "wealth" in current year

    // Calcs
    val tg = y.map(_._2).sum
    val wg = weightedGrowth(y)
    val inclusiveGrowth = y.map{ case (a, b) => b/wg - a/tg } // or sort here?
    cov(inclusiveGrowth.zipWithIndex)(spark)
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
    // the (0) index is the "wealth" part of the raster, we ignore the "population"
    val l = a.zip(b).map{ case ((k, v1), (_, v2)) => (k, (v1(0), v2(0)))}

    // skip key of 0????
    val keys = a.keys
      .filter(_ != 0)
      .distinct
      .collect.toList

    val growthRates = keys.map{k =>
      println(s"nandan! Calculating growth for country code: ${k}")
      growth(l.filter(_._1 == k).values)
    }

    spark.sparkContext
      .parallelize(keys.zip(growthRates))
      .map{ case (a,b) => GrowthRates(a,b)}
      .toDS
  }
}

// 128
// +----+--------------------+
// |code|                gini|
// +----+--------------------+
// | 122|  0.5985912285723316|
// |   4|  -319.5529404093613|
// | 166| -0.0795011980318776|
// | 248| 0.49720438750227913|
// | 229| -3366.9273441849145|
// | 169|   0.796593396595199|
// |  95|  0.6273615212018973|
// |   7|  0.8148704896817094|
// |  85|  0.7716855130129261|
// | 199|2.118373959092423...|
// +----+--------------------+

// 256
// +----+------------------+
// |code|              gini|
// +----+------------------+
// | 122|0.7502577537725301|
// |   4|0.7535888662969228|
// | 229|0.9557211562932935|
// | 169|0.7965933965524528|
// |  95|0.6273615212018977|
// | 166|0.6519216221889899|
// |   7|0.8148704896817804|
// | 248|0.6785285357145767|
// |  85|0.7070609600486932|
// | 199|0.7399530878901714|
// +----+------------------+

// 512
// +----+------------------+
// |code|              gini|
// +----+------------------+
// | 122|0.7502577537725301|
// |   4|0.7535888663405785|
// | 229| 0.814398089874885|
// | 169|0.7965933965533623|
// |  95|0.6273615212018977|
// | 166|0.6519216221889899|
// |   7|0.8148704896817804|
// | 248|0.6785285357218527|
// |  85|0.7070609600996249|
// | 199|0.7399530878828955|
// +----+------------------+

// 768
// +----+------------------+
// |code|              gini|
// +----+------------------+
// | 122|0.7502577537725301|
// |   4|0.7535888662969228|
// | 229|0.8143980897584697|
// | 169|0.7965933965524528|
// |  95|0.6273615212018977|
// | 166|0.6519216221889899|
// |   7|0.8148704896817804|
// | 248|0.6785285357182147|
// |  85|0.7070609600777971|
// | 199|0.7399530878828955|
// +----+------------------+


// 2 49 7.5 54

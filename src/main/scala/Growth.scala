package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import geotrellis.shapefile.ShapeFileReader
import geotrellis.spark._
import geotrellis.raster._
import org.apache.spark.sql.{Dataset, SparkSession}
import geotrellis.vector._

import GroupByShape._
import IO._
import Wealth._

object Growth {
  def main(args: Array[String]) {

    if (args.length != 10) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <tilePath> path to tiles
        |  <shapePath> path to ShapeFile for aggregations
        |  <shapeId> Id in shapefile (must be Int!!)
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
    val Array(tilePath, shapePath, shapeId, maxTileSize, nlKeyA, nlKeyB, popKey, crush, topCode, outFile) = args
    // val Array(tilePath, shapePath, maxTileSize, nlKeyA, nlKeyB, popKey, crush, topCode, outFile) = Array("upf-inequality-raw-geotifs", "./municipalities/2006/Election2006_Municipalities.shp", "16384", "2012.tif", "2013.tif", "2015.tif", "4", "99999999", "growth-out-2")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Pipeline")
      .getOrCreate()

    implicit val sc : SparkContext = spark.sparkContext

    val tileSize = maxTileSize.toInt
    val Seq(nlA, nlB, pop) = Seq(nlKeyA, nlKeyB, popKey).map(readRDD(tilePath, _, tileSize))

    val countries : Seq[MultiPolygonFeature[Map[String, AnyRef]]] = ShapeFileReader.readMultiPolygonFeatures(shapePath)

    // change this to actually work with our country dataset!
    val shapes = getId(sc.parallelize(countries.take(20)), shapeId)

    val wA = wealthRaster(nlA, pop, crush.toDouble, topCode.toDouble)
    val wB = wealthRaster(nlB, pop, crush.toDouble, topCode.toDouble)
    growth(wA, wB, shapes).coalesce(1).write.csv(outFile)
  }

  // weighted growth is \sum \frac{y^2_{nrt}}{y_nrt-1}
  def weightedGrowth(years: RDD[(Double, Double)]) : Double = {
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
  def growth(years: RDD[(Double, Double)])(implicit spark: SparkSession) : Double = {

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
    shapes: RDD[Feature[G, Int]]
  )(implicit spark: SparkSession) : Dataset[GrowthRates] = {
    import spark.implicits._
    val Seq(a,b) = Seq(wealthA, wealthB).map(groupByVectorShapes(shapes, _))

    val l = a.join(b)
      .map{ case (k, (sa, sb)) => (k, (sa(0), sb(0)))} // Get only the wealth (first band)

    val keys = a.keys.distinct.collect.toList
    val growthRates = keys.map(k => growth(l.filter(_._1 == k).values))
    spark.sparkContext.parallelize(keys.zip(growthRates)).map{ case (a,b) => GrowthRates(a,b)}.toDS
  }
}

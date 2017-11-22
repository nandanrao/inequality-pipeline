package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import geotrellis.spark._
import geotrellis.raster._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.{StorageLevel}
import geotrellis.vector._

import edu.upf.inequality.pipeline.GroupByShape._
import edu.upf.inequality.pipeline.IO._
import edu.upf.inequality.pipeline.Growth._
import edu.upf.inequality.pipeline.Wealth._

import GroupByShape._
import IO._
import Wealth._


object Growth {
  def main(args: Array[String]) {

    if (args.length != 13) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <bucket> S3 bucket that prefixes everything, "false" for local file system
        |  <shapeKey> path to ShapeFile or Raster file of shapes for aggregations
        |  <shapeId> Id field in shapefile ("false" if we are using raster data)
        |  <maxTileSize> max tile size (Geotrellis file-reading option)
        |  <layoutSize> size of floatingLayoutScheme (Geotrellis tiling option)
        |  <numPartitions> partitions for raster RDDS - false if none (Geotrellis default of 1!)
        |  <nlKeyA> Nightlight year 1 file path
        |  <nlKeyB> Nightlight year 2 file path
        |  <popKey> Population file path
        |  <popKey> code of desired population in ETL database
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }
    val Array(bucket, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKeyA, nlKeyB, popKey, crush, topCode, outFile) = args

    // For interactive
    // val Array(tilePath, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKeyA, nlKeyB, popKey, crush, topCode, outFile) = Array("upf-inequality-raw-geotifs", "simple/countries-europe.tif", "false", "256", "512", "1024", "simple/nl-2012-europe.tif", "simple/nl-2013-europe.tif", "simple/pop-2013-europe.tif", "4", "99999999", "s3://upf-inequality-raw-geotifs/growth-calcs-6-7")


    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Growth")
      .getOrCreate()
    implicit val sc : SparkContext = spark.sparkContext

    // Parse CLI args
    val tilePath = if (bucket == "false") None else Some(bucket)
    val Seq(tileSize, layout) = Seq(maxTileSize, layoutSize).map(_.toInt)
    val partitions = if (numPartitions == "false") None else Some(numPartitions.toInt)

    // Read population RDD
    val pop = readRDD(tilePath, popKey, tileSize, layout, partitions)(sc)

    // Read nightlight as RDD and create Wealth Rasters
    val Seq(wA, wB) = Seq(nlKeyA, nlKeyB)
      .map(readRDD(tilePath, _, tileSize, layout, partitions)(sc))
      .map(wealthRaster(_, pop, crush.toFloat, topCode.toFloat)) // use two pops for wealths!!

    // Read shapes file (vector or raster)
    val shapes = if (shapeId != "false") readShapeFile(tilePath, shapeKey, shapeId, wA.metadata)(sc) else readRDD(tilePath, shapeKey, tileSize, layout, partitions)(sc)

    // Calculate and write!
    growth(wA, wB, shapes).coalesce(1).write.csv(outFile)
  }

  // weighted growth is \sum \frac{y^2_{nrt}}{y_nrt-1}
  def weightedGrowth(years: RDD[(Float, Float)]) : Double = {
    years.map{ case (a,b) => b*b / a  }.sum
  }

  // weighted growth is \sum \frac{y^2_{nrt}}{y_nrt-1}
  def weightedGrowth2(years: RDD[(BigDecimal, BigDecimal)]) : BigDecimal = {
    years.map{ case (a,b) => b*b / a  }.reduce(_ + _)
  }

  // Used for writing to CSV's
  case class DI(growth:Double, index: Long)
  case class GrowthRates(code: Int, growth:Double)

  //
  def cov(rdd: RDD[(Double, Long)])(implicit spark: SparkSession) : Double = {
    import spark.implicits._
    rdd.map{ case (d,i) => DI(d,i) }.toDF.stat.cov("growth", "index")
  }

  case class DI2(growth:BigDecimal, index: Long)
  def covBigDec(rdd: RDD[(BigDecimal, Long)])(implicit spark: SparkSession) : BigDecimal = {
    import spark.implicits._
    rdd.map{ case (d,i) => DI2(d,i) }.toDF.stat.cov("growth", "index")
  }

  /** Takes a simple RDD of a tuple: one years nl/cap and the prev.
    * Returns a single value. Currently a BigDecimal to deal with instability.
    */
  def growth(years: RDD[(Float, Float)])(implicit spark: SparkSession) : BigDecimal = {
    val mc = new java.math.MathContext(256)

    // Filter years that are NaN
    val y = years
      .filter{ case (a,b) => !a.isNaN && !b.isNaN }
      .sortBy(_._2) // sort by "wealth" in current year
      .map{ case (a,b) => (a*100, b*100)}
      .map{ case (a,b) => (BigDecimal(a, mc), BigDecimal(b, mc))}

    // return 0 for empty RDD's, otherwise will throw!
    if (y.isEmpty) return BigDecimal(0, mc)

    // we use y multiple times, so cache it
    y.cache()

    // Calcs
    val tg = y.map(_._2).reduce(_ + _)
    val wg = weightedGrowth2(y)
    val inclusiveGrowth = y.map{ case (a, b) => b/wg - a/tg }
    covBigDec(inclusiveGrowth.zipWithIndex)(spark)
  }

  /** Primary calculation class, returns growth given Wealth rasters and shapes!
    */
  def growth[G <: Geometry](
    wealthA: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    wealthB: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    shapes: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]
  )(implicit spark: SparkSession) : Dataset[GrowthRates] = {

    import spark.implicits._
    val Seq(a,b) = Seq(wealthA, wealthB).map(groupByRasterShapes(shapes, _))

    // Both are products of leftOuterJoin on shapes, so we zip together to combine the keys.
    // The (0) index is the "wealth" part of the raster, we ignore the "population"
    val l = a
      .zip(b)
      .map{ case ((k, v1), (_, v2)) => (k, (v1(0), v2(0)))}
      .repartition(wealthA.partitions.length)

    // .persist
    l.persist(StorageLevel.MEMORY_AND_DISK_SER)

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
      .map{ case (a,b) => GrowthRates(a,b.toDouble)}
      .toDS
  }
}

// Results are unstable!

// +----+--------------------+
// |code|              growth|
// +----+--------------------+
// | 122|-0.01028878405793...|
// |   4|-0.00918841020230...|
// | 229|-0.16324617223839397|
// | 169|-0.03963957377885159|
// |  95|0.001676121941737...|
// | 166|-5.50689284732785...|
// |   7|-0.01756434758658...|
// | 248|-0.01174583334111...|
// |  85| -0.1164307605889989|
// | 199|0.002670610425490137|
// +----+--------------------+

// +----+--------------------+
// |code|              growth|
// +----+--------------------+
// | 122|-0.16404171671841267|
// |   4|-0.00918841020230...|
// | 229|-0.17182565528120336|
// | 169|-0.03963957377885159|
// |  95|0.001676121941737...|
// | 166|  0.7904330845717524|
// |   7|-0.01756434758658...|
// | 248| -0.1009427870690889|
// |  85|-0.14319069122371414|
// | 199|-0.09041687719085308|
// +----+--------------------+

// +----+--------------------+
// |code|              growth|
// +----+--------------------+
// | 122| 0.02133221248827066|
// |   4|-0.21089518015046158|
// | 229|-0.18373785803761794|
// | 169|-0.03963957377885159|
// |  95|0.001676121941737...|
// | 166|  0.7904330845717524|
// |   7|                -0.0|
// | 248| -0.1009427870690889|
// |  85|-0.13533412701097827|
// | 199|-0.09041687719085308|
// +----+--------------------+

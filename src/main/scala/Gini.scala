package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.Try
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner

import com.redhat.et.silex.rdd.drop.implicits._
import geotrellis.spark._
import geotrellis.raster._

import Wealth._
import IO._
import GroupByShape._

object Gini {

  def main(args: Array[String]) {

    if (args.length != 11) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <tilePath> path to tiles
        |  <shapeKey> path to ShapeFile for aggregations
        |  <shapeId> Id in shapefile ("false" if we are using raster data?)
        |  <maxTileSize> max tile size
        |  <layoutSize> size of floatingLayoutScheme
        |  <numPartitions> partitions for raster RDDS
        |  <nlKey> key of n
        |  <popKey> code of desired population in ETL database
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }
    val Array(tilePath, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKey, popKey, crush, topCode, outFile) = args

    // val Array(tilePath, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKey, popKey, crush, topCode, outFile) = Array("upf-inequality-raw-geotifs", "simple/countries-europe.tif", "false", "256", "512", "1024", "simple/nl-2013-europe.tif", "simple/pop-2013-europe.tif", "4", "99999999", "s3://upf-inequality-raw-geotifs/growth-calcs-6-7")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Gini")
      .getOrCreate()
    implicit val sc : SparkContext = spark.sparkContext

    val Seq(tileSize, layout, partitions) = Seq(maxTileSize, layoutSize, numPartitions).map(_.toInt)
    val pop = readRDD(tilePath, popKey, tileSize, layout, partitions)

    val wealth = wealthRaster(readRDD(tilePath, nlKey, tileSize, layout, partitions), pop, crush.toFloat, topCode.toFloat)

    val shapes = if (shapeId != "false") readShapeFile(tilePath, shapeKey, shapeId, wealth.metadata) else readRDD(tilePath, shapeKey, tileSize, layout, partitions)

    gini(wealth, shapes).coalesce(1).write.csv(outFile)
  }

  // Our division return 0.0 if denomenator is 0 in this case
  def div (n:Double, d:Double) : Double = {
    Try(n/d).getOrElse(0.0) match {
      case x if (x.isNaN) => 0.0
      case x if (x.isInfinite) => 0.0
      case x => x
    }
  }

  val mult = (t: Tuple2[Double, Double]) => t._1*t._2

  def unweightedGini(
    wealth: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]
  )(implicit sc: SparkContext) : Double = {

    val nl = wealth.flatMap{ case (k, t) => t.bands(0).toArrayDouble }
    baseUnweightedGini(nl)
  }

  def baseUnweightedGini(nl: RDD[Double])(implicit sc: SparkContext) : Double = {
    val partitions = nl.partitions.length
    val repartitioned = nl.repartition(partitions)
    baseGini(nl, sc.parallelize(Seq.fill(nl.count.asInstanceOf[Int])(1.toFloat), partitions))
  }

  case class GiniCalculation(code: Int, gini:Double)

  def gini(
    wealth: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    shapes: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]
  )(implicit spark: SparkSession) : Dataset[GiniCalculation] = {

    val grouped : RDD[(Int, Seq[Double])] =
      groupByRasterShapes(shapes, wealth).mapValues{ v => v.map(_.toDouble) }

    val keys = grouped.keys
      .filter(_ != 0)
      .distinct
      .collect.toList

    // divide keys into two groups by size: do a groupByKey and map for small,
    // and do this sequential distributed number for the big uns.

    val giniRates = keys.map{k =>
      println(s"nandan! Calculating gini for country code: ${k}")
      val g = grouped.filter(_._1 == k).values
      val (w, pop) = (g.map(_(0)), g.map(_(1)))
      baseGini(w, pop)
    }

    import spark.implicits._
    spark.sparkContext
      .parallelize(keys.zip(giniRates))
      .map{ case (a,b) => GiniCalculation(a,b)}
      .toDS
  }

  def baseGini(w: RDD[Double], pop: RDD[Double]) : Double = {
    val cleaned = w.zip(pop)
      .filter{ case (w,p) => !w.isNaN && !p.isNaN }
      .filter{ case (w,p) => p >= 1.0 }

    val sumOfPop = cleaned.map(_._2).sum

    // (Nightlight per Population, Weights)
    val zipped = cleaned
      .map{ case (w, p) => (w, div(p,sumOfPop)) }
      .sortBy(_._1)

    val weightedNl = zipped.map(mult)
    val sumOfWeightedVals = weightedNl.sum

    val nu = cumsum(weightedNl).map(div(_, sumOfWeightedVals))
    val p = cumsum(zipped.map(_._2))

    shiftzip(nu, p).map(mult).sum - shiftzip(p, nu).map(mult).sum
  }

  // gini with scala arrays!

  def shiftzip[T: ClassTag, R: ClassTag](
    one: RDD[T],
    two: RDD[R]
  ) : RDD[(T, R)] = {
    val partitioner = new HashPartitioner(one.partitions.length)

    val a = one.zipWithIndex.drop(1).map{ case (v,i) => (v, i - 1) }.map{ case (v,i) => (i, v)}
    val b = two.zipWithIndex.dropRight(1).map{ case (v,i) => (i, v)}

    a.partitionBy(partitioner).values
      .zip(b.partitionBy(partitioner).values)
  }

  import com.redhat.et.silex.rdd.scan.implicits._
  def cumsum(rdd: RDD[Double]) = rdd.scanLeft(0.0)(_+_)
}

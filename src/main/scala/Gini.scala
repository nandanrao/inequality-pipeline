package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.Try
import scala.reflect._
import org.apache.spark.HashPartitioner

import com.redhat.et.silex.rdd.drop.implicits._
import geotrellis.spark._
import geotrellis.raster._

// import edu.upf.inequality.pipeline.GroupByShape._
// import edu.upf.inequality.pipeline.IO._
// import edu.upf.inequality.pipeline.Gini._
// import edu.upf.inequality.pipeline.Wealth._

// val Array(tilePath, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKey, popKey, crush, topCode, outFile) = Array("upf-inequality-raw-geotifs", "simple/countries-small.tif", "false", "256", "256", "1024", "simple/nl-2013-small.tif", "simple/pop-2013-small.tif", "4", "99999999", "s3://upf-inequality-raw-geotifs/growth-calcs-6-7")

import Wealth._
import IO._
import GroupByShape._


object Gini {

  def main(args: Array[String]) {

    if (args.length != 11) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <bucket> S3 bucket that prefixes everything, "false" for local file system
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
    val Array(bucket, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKey, popKey, crush, topCode, outFile) = args

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Gini")
      .getOrCreate()
    implicit val sc : SparkContext = spark.sparkContext

    val tilePath = if (bucket == "false") None else Some(bucket)

    val Seq(tileSize, layout, partitions) = Seq(maxTileSize, layoutSize, numPartitions).map(_.toInt)
    val pop = readRDD(tilePath, popKey, tileSize, layout, partitions)(sc)

    val wealth = wealthRaster(readRDD(tilePath, nlKey, tileSize, layout, partitions)(sc), pop, crush.toFloat, topCode.toFloat)

    val shapes = if (shapeId != "false") readShapeFile(tilePath, shapeKey, shapeId, wealth.metadata)(sc) else readRDD(tilePath, shapeKey, tileSize, layout, partitions)(sc)

    gini(wealth, shapes)(spark).coalesce(1).write.csv(outFile)
  }

  // Our division return 0.0 if denomenator is 0 in this case
  def div (n:Double, d:Double) : Double = {
    Try(n/d).getOrElse(0.0) match {
      case x if (x.isNaN) => 0.0
      case x if (x.isInfinite) => 0.0
      case x => x
    }
  }

  // BigDecimal!
  def div (n:BigDecimal, d:BigDecimal) : BigDecimal = {
    Try(n/d).getOrElse(BigDecimal(0.0))
  }

  def unweightedGini(
    wealth: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]
  )(implicit sc: SparkContext) : Double = {

    val nl = wealth.flatMap{ case (k, t) => t.bands(0).toArrayDouble }
    baseUnweightedGini(nl)
  }

  def baseUnweightedGini(nl: RDD[Double])(implicit sc: SparkContext) : Double = {
    val partitions = nl.partitions.length
    val repartitioned = nl.repartition(partitions)
    baseGini(nl, sc.parallelize(Seq.fill(nl.count.asInstanceOf[Int])(1.0), partitions))
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

    // should we sortBy here, rather than resort every time in this inner loop???
    val sorted = grouped.sortBy(t => t._2(0))
    val giniRates = keys.map{k =>
      println(s"nandan! Calculating gini for country code: ${k}")
      val g = sorted.filter(_._1 == k).values
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
    // val mc = new java.math.MathContext(256)

    val cleaned = w.zip(pop)
      .filter{ case (w,p) => !w.isNaN && !p.isNaN }
      .filter{ case (w,p) => p >= 1.0 }
      // .map{ case (w,p) => (BigDecimal(w, mc), BigDecimal(p, mc))}

    val sumOfPop = cleaned.map(_._2).sum

    // (Nightlight per Population, Weights)
    val zipped = cleaned
      .map{ case (w, p) => (w, Try(p/sumOfPop).getOrElse(0.0)) }
      .sortBy(_._1)

    val weightedNl = zipped.map{ case (a,b) => a*b}
    val sumOfWeightedVals = weightedNl.sum

    val nu = cumsum(weightedNl).map(div(_, sumOfWeightedVals))
    val p = cumsum(zipped.map(_._2))

    shiftedCalculations(nu, p, multsum) - shiftedCalculations(p, nu, multsum)
  }

  def multsum(a: Seq[Double], b: Seq[Double], default: Double) : Double = {
    Try(a.zip(b).map{ case (a,b) => a * b}.reduce(_ + _)).getOrElse(default)
  }

  def multsum(a: Seq[Double], b: Seq[Double]) : Double = {
    multsum(a, b, 0.0)
  }

  def shiftedCalculations(
    one: RDD[Double],
    two: RDD[Double],
    fn: (Seq[Double], Seq[Double]) => Double
  ) : Double = {

    val rdd = one
      .zip(two)
      .mapPartitions{ iter => {
        if (iter.isEmpty) {
          Iterator[(Double, Double, Double)]()
        } else {
          val l = iter.toList
          val a = l.map(_._1)
          val b = l.map(_._2)

          // if we don't have anything after dropping, return 0.0
          val calc = fn(a.drop(1), b.dropRight(1))
          Iterator((a.head, b.last, calc))
        }
    }}

    val a = rdd.map(_._1).collect
    val b = rdd.map(_._2).collect
    val extra = rotateAndApply(a,b,fn)

    rdd.map(_._3).collect.reduce(_ + _) + extra
  }

  def rotateAndApply[T: ClassTag, A: ClassTag](a: Seq[T], b: Seq[T], fn: (Seq[T], Seq[T]) => A) : A = {
    val newA = a.view.drop(1) ++ a.view.take(1)
    fn(newA, b)
  }
  // gini with scala arrays!

  import com.redhat.et.silex.rdd.scan.implicits._
  // implicit val bigDecimalClassTag = classTag[RDD[BigDecimal]]
  def cumsum(rdd: RDD[Double]) = rdd.scanLeft(0.0)(_+_)
  // def cumsum(rdd: RDD[BigDecimal]) = rdd.scanLeft(BigDecimal(0.0))(_+_)
}

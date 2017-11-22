package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.{StorageLevel}
import scala.util.Try
import scala.reflect._
import org.apache.spark.HashPartitioner

import com.redhat.et.silex.rdd.scan.implicits._
import com.redhat.et.silex.rdd.drop.implicits._
import geotrellis.spark._
import geotrellis.raster._

import Wealth._
import IO._
import GroupByShape._


object Gini {

  def main(args: Array[String]) {

    if (args.length != 12) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <bucket> S3 bucket that prefixes everything, "false" for local file system
        |  <shapeKey> path to ShapeFile or Raster file of shapes for aggregations
        |  <shapeId> Id field in shapefile ("false" if we are using raster data)
        |  <maxTileSize> max tile size (Geotrellis file-reading option)
        |  <layoutSize> size of floatingLayoutScheme (Geotrellis tiling option)
        |  <numPartitions> partitions for raster RDDS - false if none (Geotrellis default of 1!)
        |  <nlKey> Nightlight file path
        |  <popKey> Population file path
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <networkTimeout> Spark network.timeout value
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }

    val Array(bucket, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKey, popKey, crush, topCode, networkTimeout, outFile) = args

    // For interactive
    // val Array(tilePath, shapeKey, shapeId, maxTileSize, layoutSize, numPartitions, nlKey, popKey, crush, topCode, outFile) = Array("upf-inequality-raw-geotifs", "simple/countries-small.tif", "false", "256", "256", "1024", "simple/nl-2013-small.tif", "simple/pop-2013-small.tif", "4", "99999999", "s3://upf-inequality-raw-geotifs/growth-calcs-6-7")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Gini")
      .config("spark.network.timeout", networkTimeout)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    implicit val sc : SparkContext = spark.sparkContext

    // parse CLI args
    val tilePath = if (bucket == "false") None else Some(bucket)
    val Seq(tileSize, layout) = Seq(maxTileSize, layoutSize).map(_.toInt)
    val partitions = if (numPartitions == "false") None else Some(numPartitions.toInt)

    // Read population raster
    val pop = readRDD(tilePath, popKey, tileSize, layout, partitions)(sc)

    // Read nightlight raster and create wealth raster
    val wealth = wealthRaster(readRDD(tilePath, nlKey, tileSize, layout, partitions)(sc), pop, crush.toFloat, topCode.toFloat)

    // Read shapes file (vector or raster)
    val shapes = if (shapeId != "false") readShapeFile(tilePath, shapeKey, shapeId, wealth.metadata)(sc) else readRDD(tilePath, shapeKey, tileSize, layout, partitions)(sc)

    // Calculate and write!
    gini(wealth, shapes)(spark).coalesce(1).write.csv(outFile)
  }

  // Our division return 0.0 if denomenator is!
  // TODO: Needed?
  def div (n:Double, d:Double) : Double = {
    Try(n/d).getOrElse(0.0) match {
      case x if (x.isNaN) => 0.0
      case x if (x.isInfinite) => 0.0
      case x => x
    }
  }

  /** Unweighted versions here for completeness.
    * NOTE: These haven't been used, might be out-of-date
    */
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

  // Case Class just used to print to CSV easily
  case class GiniCalculation(code: Int, gini:Double)

  /** Calculates Gini given wealth raster and shapes as RDD.
    * Note: This is the main class of this object!
    */
  def gini(
    wealth: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    shapes: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]
  )(implicit spark: SparkSession) : Dataset[GiniCalculation] = {

    val grouped : RDD[(Int, Seq[Double])] =
      groupByRasterShapes(shapes, wealth).mapValues{ v => v.map(_.toDouble) }

    // cache this because we'll hit it for each loop...
    grouped.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val keys = grouped.keys
      .filter(_ != 0)
      .distinct
      .collect.toList

    // divide keys into two groups by size: do a groupByKey and map for small,
    // and do this sequential distributed number for the big uns.
    val giniRates = keys.map{k =>
      println(s"nandan! Calculating gini for country code: ${k}")
      val g = grouped.filter(_._1 == k).values // .reparitition for rebalancing??
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
    // TODO: Caching vs disk storage?

    val cleaned = w.zip(pop)
      .filter{ case (w,p) => !w.isNaN && !p.isNaN }
      .filter{ case (w,p) => p >= 1.0 }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val sumOfPop = cleaned.map(_._2).sum

    // (Nightlight per Population, Weights)
    val zipped = cleaned
      .map{ case (w, p) => (w, Try(p/sumOfPop).getOrElse(0.0)) }
      .sortBy(_._1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)


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

  /** Applies a function to 2 RDDs, but rather than a direct map, it shifts
    * the head of one RDD to the tail. This is comlex due to the nature of RDD's!
    */
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

          // In each partition, we drop the head of one and the tail of the
          // other, and calculate the function on these Seq's locally.
          // We then collect all the "extras" -- the dropped heads and tails,
          // and calculate those separately (the tail of one partition against
          // the head of the next).
          val calc = fn(a.drop(1), b.dropRight(1))
          Iterator((a.head, b.last, calc))
        }
    }}

    val a = rdd.map(_._1).collect
    val b = rdd.map(_._2).collect
    val extra = rotateAndApply(a,b,fn)

    rdd.map(_._3).collect.reduce(_ + _) + extra
  }

  /** Applies a func to two Seqs, but shifts the head of one to the end first.
    * This is used in calculating the "extras" left over from shitedCalculations.
    */
  def rotateAndApply[T: ClassTag, A: ClassTag](
    a: Seq[T], b: Seq[T],
    fn: (Seq[T], Seq[T]) => A
  ) : A = {
    val newA = a.view.drop(1) ++ a.view.take(1)
    fn(newA, b)
  }

  /** Implement cumsum using redhat implicits */
  def cumsum(rdd: RDD[Double]) = rdd.scanLeft(0.0)(_+_)
}

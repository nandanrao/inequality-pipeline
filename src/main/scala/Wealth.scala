package edu.upf.inequality.pipeline

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import geotrellis.spark._
import geotrellis.vector._

import geotrellis.spark.io._

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.writer._

import GroupByShape._
import Gini._
import Implicits._
import IO._

object Wealth {

  def main(args: Array[String]) {
    if (args.length != 9) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <bucket> S3 bucket that prefixes everything, "false" for local file system
        |  <maxTileSize> max tile size (Geotrellis file-reading option)
        |  <layoutSize> size of floatingLayoutScheme (Geotrellis tiling option)
        |  <numPartitions> partitions for raster RDDS - false if none (Geotrellis default of 1!)
        |  <nlKey> Nightlight file path
        |  <popKey> Population file path
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }

    val Array(bucket, maxTileSize, layoutSize, numPartitions, nlKey, popKey, crush, topCode, outFile) = args

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Wealth")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      .getOrCreate()
    implicit val sc : SparkContext = spark.sparkContext

    val tilePath = if (bucket == "false") None else Some(bucket)

    val Seq(tileSize, layout) = Seq(maxTileSize, layoutSize).map(_.toInt)
    val partitions = if (numPartitions == "false") None else Some(numPartitions.toInt)

    val Seq(nl, pop) = Seq(nlKey, popKey)
      .map(readRDD(tilePath, _, tileSize, layout, partitions))

    writeTif(wealthRaster(nl, pop, crush.toFloat, topCode.toFloat), outFile)
  }

  /** Calculates wealth-per-population for each pixel, returns ContextRDD.
    * Takes care of all coding issues along the way!
    */
  def wealth(
    nl: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    pop: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    crush: Float,
    topCode: Float
  ) = {

    val minLight =  1.0 // We don't want any 0 NL values! TODO: move this!

    nl.withContext{ _.mapValues(_.localAdd(minLight)) }
      .spatialJoin(pop.withContext{
        _.mapValues(_.localCrush(crush).localTopCode(topCode))
      })
      .withContext { _.combineValues( _ localDivide _) }
      .mapContext{ bounds => nl.metadata }
  }

  /** Creates our Wealth raster, returns as a Context RDD
    * Creates a Multiband raster file with wealth, population as the two bands.
    */

  def wealthRaster(
    nl: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    pop: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    crush: Float,
    topCode: Float
  ) : ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]= {

    wealth(nl, pop, crush, topCode)
      .spatialJoin(pop)
      .withContext( _.combineValues(MultibandTile(_, _)))
      .mapContext{ bounds => nl.metadata }
  }

  def writeTif(
    rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]],
    uri: String
  )(implicit sc: SparkContext) : Unit = {

    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(uri), sc.hadoopConfiguration)
    val stream = fs.create(new org.apache.hadoop.fs.Path(uri))
    val tif = GeoTiff(rdd.stitch.crop(rdd.metadata.extent), rdd.metadata.crs)

    try {
      new GeoTiffWriter(tif, stream).write()
    } finally {
      stream.close()
    }
  }
}

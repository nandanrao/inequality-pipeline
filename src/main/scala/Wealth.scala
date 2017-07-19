package edu.upf.inequality.pipeline

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import geotrellis.spark._
import geotrellis.vector._

import geotrellis.spark.io._

import geotrellis.raster._
import geotrellis.raster.io.geotiff._

import GroupByShape._
import Gini._
import Implicits._
import Wealth._
import IO._

object Wealth {

  def main(args: Array[String]) {
    if (args.length != 8) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <tilePath> path to tiles
        |  <maxTileSize> maxTileSize for
        |  <numPartitions> partitions for raster RDDs
        |  <nlCode> code of desired nightlight in ETL database
        |  <popCode> code of desired population in ETL database
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }

    val Array(tilePath, maxTileSize, numPartitions, nlKey, popKey, crush, topCode, outFile) = args

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Wealth")
      .getOrCreate()
    implicit val sc : SparkContext = spark.sparkContext

    val Seq(nl, pop) = Seq(nlKey, popKey)
      .map(readRDD(tilePath, _, maxTileSize.toInt, numPartitions.toInt))

    writeTif(wealthRaster(nl, pop, crush.toFloat, topCode.toFloat), outFile)
  }

  def wealth(
    nl: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    pop: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    crush: Float,
    topCode: Float
  ) = {

    val minLight =  1.0 // We don't want any 0 NL values!

    nl.withContext{ _.mapValues(_.localAdd(minLight)) }
      .spatialJoin(pop.withContext{
        _.mapValues(_.localCrush(crush).localTopCode(topCode))
      })
      .withContext { _.combineValues( _ localDivide _) }
      .mapContext{ bounds => nl.metadata }
  }

  def wealthRaster(
    nl: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    pop: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    crush: Float,
    topCode: Float
  ) : ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]= {
    // create a multiband RDD that includes the population

    wealth(nl, pop, crush, topCode)
      .spatialJoin(pop)
      .withContext( _.combineValues(MultibandTile(_, _)))
      .mapContext{ bounds => nl.metadata }
  }

  def writeTif(
    rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]],
    f: String) : Unit = {
    GeoTiff(rdd.stitch.crop(rdd.metadata.extent), rdd.metadata.crs).write(f)
  }
}

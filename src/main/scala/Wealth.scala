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
    if (args.length != 7) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <tilePath> path to tiles
        |  <maxTileSize> maxTileSize for
        |  <nlCode> code of desired nightlight in ETL database
        |  <popCode> code of desired population in ETL database
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }

    val Array(tilePath, maxTileSize, nlKey, popKey, crush, topCode, outFile) = args

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Wealth")
      .getOrCreate()
    implicit val sc : SparkContext = spark.sparkContext

    val nl = readRDD(tilePath, nlKey, maxTileSize.toInt)
    val pop = readRDD(tilePath, popKey, maxTileSize.toInt)
    writeTif(wealthRaster(nl, pop, crush.toDouble, topCode.toDouble), outFile)
  }

  def wealth(
    nl: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    pop: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    crush: Double,
    topCode: Double
  ) = {

    val minLight =  1 // We don't want any 0 NL values!

    nl.withContext{ _.mapValues(_.convert(FloatCellType).localAdd(minLight)) }
      .spatialJoin(pop.withContext{
        _.mapValues(_.localCrush(crush).localTopCode(topCode))
      })
      .withContext { _.combineValues( _ localDivide _) }
      .mapContext{ bounds => TileLayerMetadata(FloatCellType, nl.metadata.layout, nl.metadata.extent, nl.metadata.crs, nl.metadata.bounds )}
  }

  def wealthRaster(
    nl: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    pop: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    crush: Double,
    topCode: Double
  ) : ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]= {
    // create a multi-layer RDD that includes the population

    wealth(nl, pop, crush, topCode)
      .spatialJoin(pop)
      .withContext( _.combineValues(MultibandTile(_, _)))
      .mapContext{ bounds => TileLayerMetadata(FloatCellType, nl.metadata.layout, nl.metadata.extent, nl.metadata.crs, nl.metadata.bounds )}
  }

  def writeTif(
    rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]],
    f: String) : Unit = {
    GeoTiff(rdd.stitch.crop(rdd.metadata.extent), rdd.metadata.crs).write(f)
  }
}

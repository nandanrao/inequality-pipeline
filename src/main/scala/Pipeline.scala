package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io._
import geotrellis.shapefile.ShapeFileReader
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._

import GroupByShape._
import Gini._
import Implicits._

object Pipeline {
  def main(args: Array[String]) {

    if (args.length != 7) {
      System.err.println(s"""
        |Usage: Indexer <mobile>
        |  <tilePath> path to tiles
        |  <shapePath> path to ShapeFile for aggregations
        |  <nlCode> code of desired nightlight in ETL database
        |  <popCode> code of desired population in ETL database
        |  <crush> Lower limit of population value to crush to 0
        |  <topCode> Upper limit of population value to topcode
        |  <outFile> name of file to print out
        """.stripMargin)
      System.exit(1)
    }
    val Array(tilePath, shapePath, nlCode, popCode, crush, topCode, outFile) = args
    // val Array(tilePath, shapePath, nlCode, popCode) = Array("/Users/nandanrao/Documents/BGSE/inequality/pipeline/etl/tiles", "./municipalities/2006/Election2006_Municipalities.shp", "nl_2013", "pop_2015")

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Pipeline")
      .getOrCreate()

    implicit val sc : SparkContext = spark.sparkContext

    // switch for S3!
    // Read from the database created by ETL process (Hadoop FS)
    def makeRDD(layerName: String, path: String, num: Int) = {
      val inLayerId = LayerId(layerName, num)
      require(HadoopAttributeStore(path).layerExists(inLayerId))

      HadoopLayerReader(path)
        .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](inLayerId)
        .result
    }

    // // We use this to query the ETL DB, avoids reading tiles that we won't use.
    // val countries : Seq[MultiPolygonFeature[Map[String, AnyRef]]] = ShapeFileReader.readMultiPolygonFeatures(shapePath)
    val nl = makeRDD(nlCode, tilePath, 8)
    val pop = makeRDD(popCode, tilePath, 8)
    // val countriesRDD = getId(sc.parallelize(countries.take(20)), "MUNICID") 
    
    // val nlGrouped = groupByVectorShapes(countriesRDD, nl)
    // val popGrouped = groupByVectorShapes(countriesRDD, pop)

    // val municipalities = nlGrouped.keys.distinct.collect.toList
    // val ginis = municipalities.map{ m =>
    // //   // map over each nl/pop pair???? 
    // //   // saves having to do the grouupByShape again... although you just have to spa
    // //   // spatitional joins fo r every year, which is the same operatioooon....
    // //   // unless you can actually trust it's ETL's perfectt, then you just zip...
    // //   // Or you can make a multiband raster with data fromm every year...? eh...
    //   gini(nlGrouped.filter(_._1 == m).values, popGrouped.filter(_._1 == m).values)
    // }

    // println(municipalities.zip(ginis))

    val divd = nl                 
      .withContext{ _.mapValues(_.convert(FloatCellType)) }                  
      .spatialJoin(pop.withContext{ 
        _.mapValues(_.localCrush(crush.toDouble).localTopCode(topCode.toDouble)) 
      })
      .withContext { _.combineValues( _ localDivide _) }                  
      .mapContext{ bounds => TileLayerMetadata(FloatCellType, nl.metadata.layout, nl.metadata.extent, nl.metadata.crs, nl.metadata.bounds )}

    // write csv

    // def writeTiff(r: Raster[Tile], crs: CRS, f: String) : Unit = {
    //   GeoTiff(r, crs).write(f)
    // }

    def writeTiff(
      rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
      f: String) : Unit = {
      GeoTiff(rdd.stitch.crop(rdd.metadata.extent), rdd.metadata.crs).write(f)
    }

    // writeTiff(nl, "reg-nl")
    writeTiff(divd, outFile)
  }
}

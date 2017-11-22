package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import geotrellis.spark.tiling._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.hadoop._
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.shapefile.ShapeFileReader
import awscala._
import awscala.s3._
import org.apache.commons.io.IOUtils.copy


import GroupByShape._

object IO {

/** Reads GeoTiff file and returns Geotrellis ContextRDD.
  * Can read from either AWS S3 (if given a bucket), or from HDFS
  * (which also works for local files and therefore for Marenostrums GPFS).
  */
  def readRDD(
    bucket: Option[String],
    key: String,
    maxTileSize: Int = 256,
    layoutSize: Int = 256,
    numPartitions: Option[Int] = None
  )(implicit sc: SparkContext) = {

    val repartitioned = bucket match {

      case Some(b) => S3GeoTiffRDD
          .spatial(b, key, S3GeoTiffRDD.Options(
            maxTileSize = Some(maxTileSize),
            numPartitions = numPartitions
          ))

      case None => HadoopGeoTiffRDD
          .spatial(key, HadoopGeoTiffRDD.Options(
            maxTileSize = Some(maxTileSize),
            numPartitions = numPartitions
          ))
    }

    val (_, md) = repartitioned.collectMetadata[SpatialKey](FloatingLayoutScheme(layoutSize))
    ContextRDD(repartitioned.tileToLayout[SpatialKey](md), md)
  }

/** Multiband version of readRDD.
  * Currently only used by tests. Presumably useful for reading our Wealth rasters if
  * they are pre-created in the future.
  */
  def readMultibandRDD(path: String, maxTileSize: Option[Int] = None, numPartitions: Option[Int] = None)(implicit sc: SparkContext) = {

    val rdd = HadoopGeoTiffRDD
      .spatialMultiband(path, HadoopGeoTiffRDD.Options(
        maxTileSize = maxTileSize,
        numPartitions = numPartitions
      ))

    val layout = FloatingLayoutScheme(maxTileSize.getOrElse(256))
    val (_, md) = rdd.collectMetadata[SpatialKey](layout)
    new ContextRDD(rdd.tileToLayout[SpatialKey](md), md)
  }

 // Helper function used to read shape files, simply gets the
 // given ID field from the shape file "feature" format.
 def getId[G<: Geometry](
    shapes: RDD[Feature[G,Map[String, AnyRef]]],
    field: String
  ) : RDD[Feature[G, Int]] = {

    shapes.map(_.mapData(_(field).asInstanceOf[Int]))
  }

  // Downloads from S3, as there was no shapefile reading from
  // S3, geoTrellis only offered it from local files, so the shapefile
  // here needed to be downloaded and read locally by one executor.
  def downloadObj(b: Bucket, key: String, outFile: String)(implicit s3: S3) = {
    val s3obj: Option[S3Object] = b.getObject(key)
    s3obj.map(_.getObjectContent).map(copy(_, new java.io.FileOutputStream(outFile)))
  }

  // TODO: Make this compatable with local file system
  // and decide if we even want to read shapefiles...
  def readShapeFile(bucket: String, key: String, id: String)(implicit sc: SparkContext) : RDD[Feature[MultiPolygon, Int]] = {

    implicit val s3 = S3.at(Region.Ireland)
    val b: Option[Bucket] = s3.bucket(bucket)

    val rexp = """(.+/)(\w+)(\.\w+)$""".r
    val (p, k, e) = key match { case rexp(p,k,e) => (p,k,e) }

    b.map{ bucket =>
      bucket.objectSummaries(p+k)
        .map(_.getKey)
        .foreach{case rexp(p,k,e) => downloadObj(bucket, p+k+e, k+e)}
    } match {
        case None => throw new Exception("Failed to download/write to file")
        case _ => true
    }

    val countries : Seq[MultiPolygonFeature[Map[String, AnyRef]]] = ShapeFileReader.readMultiPolygonFeatures(k+e)

    getId(sc.parallelize(countries), id)
  }

  def readShapeFile(
    bucket: Option[String],
    key: String,
    id: String,
    md: TileLayerMetadata[SpatialKey]
  )(implicit sc: SparkContext) : RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = {

    shapeToContextRDD(readShapeFile(bucket.get, key, id), md)
  }
}

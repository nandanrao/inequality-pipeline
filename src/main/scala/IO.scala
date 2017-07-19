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

  def getId[G<: Geometry](
    shapes: RDD[Feature[G,Map[String, AnyRef]]],
    field: String
  ) : RDD[Feature[G, Int]] = {

    shapes.map(_.mapData(_(field).asInstanceOf[Int]))
  }

  def readRDD(bucket: String, key: String, maxTileSize: Int, numPartitions: Int)(implicit sc: SparkContext) = {

    val rdd = S3GeoTiffRDD
      .spatial(bucket, key,
        S3GeoTiffRDD.Options(
          maxTileSize = Some(maxTileSize),
          numPartitions = Some(numPartitions)
        ))

    val (_, md) = rdd.collectMetadata[SpatialKey](FloatingLayoutScheme(maxTileSize))
    ContextRDD(rdd.tileToLayout[SpatialKey](md), md)
  }

  def readRDD(path: String, maxTileSize: Int, numPartitions: Int)(implicit sc: SparkContext) = {

    val rdd = HadoopGeoTiffRDD
      .spatial(path, HadoopGeoTiffRDD.Options(
        maxTileSize = Some(maxTileSize),
        numPartitions = Some(numPartitions)
      ))

    val (_, md) = rdd.collectMetadata[SpatialKey](FloatingLayoutScheme(maxTileSize))
    ContextRDD(rdd.tileToLayout[SpatialKey](md), md)
  }

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
    bucket: String,
    key: String,
    id: String,
    md: TileLayerMetadata[SpatialKey]
  )(implicit sc: SparkContext) : RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = {

    shapeToContextRDD(readShapeFile(bucket, key, id), md)
  }

  def downloadObj(b: Bucket, key: String, outFile: String)(implicit s3: S3) = {
    val s3obj: Option[S3Object] = b.getObject(key)
    s3obj.map(_.getObjectContent).map(copy(_, new java.io.FileOutputStream(outFile)))
  }
}

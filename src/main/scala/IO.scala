package edu.upf.inequality.pipeline

import org.apache.spark.SparkContext
import geotrellis.spark.tiling._
import geotrellis.spark.io.s3._
import geotrellis.spark._
import geotrellis.raster._

object IO {

  def readRDD(bucket: String, key: String, maxTileSize: Int)(implicit sc: SparkContext) = {
    val rdd = S3GeoTiffRDD
      .spatial(bucket, key, S3GeoTiffRDD.Options(maxTileSize = Some(maxTileSize)))
    val (_, md) = rdd.collectMetadata[SpatialKey](FloatingLayoutScheme(1024))
    ContextRDD(rdd.tileToLayout[SpatialKey](md), md)
  }
}

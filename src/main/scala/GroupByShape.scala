package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import geotrellis.vector.{Point, Polygon, Geometry}
import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.SpatialKey._

object GroupByShape {

  def groupByShapes[T](shape: Seq[Geometry]) : RDD[(String, T)] = {
    ???
  }

  def shapeToContextRDD[G <: Geometry, D](shapes: RDD[Feature[G, D]]) : ContextRDD[SpatialKey, Seq[Tile], TileLayerMetadata[SpatialKey]] = {
    val crs = CRS.fromEpsgCode(4326)
    // make this a feature[geometry] and get id from data
    val rdd = shapes.map(shapeToTile(_, 1000, 1000, crs))
    val md = rdd.collectMetadata(FloatingLayoutScheme(256))
    ContextRDD(rdd.tileToLayout[SpatialKey](md._2), md)
  }

  def shapeToTile(vec:Geometry, cols:Int, rows:Int, crs: CRS, value:Int = 1) : Tuple2[ProjectedExtent, Tile] = {
    val ve = vec.envelope
    val re = RasterExtent(ve, cols = cols, rows = rows)
    val tile = Rasterizer.rasterizeWithValue(vec, re, value)
    (ProjectedExtent(ve, crs), tile) // don't project???
  }

  val write = (t:Tile, f:String) => t.map(i => if(i > 0) 0xFF0000FF else 0x00000000).renderPng.write(f)

}

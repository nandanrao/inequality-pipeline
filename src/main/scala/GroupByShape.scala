package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import geotrellis.vector.{Geometry, Feature, ProjectedExtent}
import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutDefinition}

import geotrellis.raster.io.geotiff._


object GroupByShape {

  def getId[G<: Geometry](
    shapes: RDD[Feature[G,Map[String, AnyRef]]],
    field: String
  ) : RDD[Feature[G, Int]] = {
    shapes.map(_.mapData(_(field).asInstanceOf[Int]))
  }

  def shapeToTile(vec:Geometry, cols:Int, rows:Int, crs: CRS, value:Int = 1) : Tuple2[ProjectedExtent, Tile] = {

    val ve = vec.envelope
    val re = RasterExtent(ve, cols = cols, rows = rows)
    val tile = Rasterizer.rasterizeWithValue(vec, re, value)
    (ProjectedExtent(ve, crs), tile) // don't project???
  }

  def shapeToContextRDD[G <: Geometry](shapes: RDD[Feature[G, Int]], md: TileLayerMetadata[SpatialKey]) : RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]= {

    val crs = CRS.fromEpsgCode(4326)
    val rdd = shapes.map(f => shapeToTile(f.geom, 1000, 1000, crs, f.data))
    val newMd : TileLayerMetadata[SpatialKey] = rdd.collectMetadata(crs, md.layout)
    ContextRDD(rdd.tileToLayout[SpatialKey](newMd), newMd)
  }

  def groupByShapes[G <: Geometry](

    shapes: RDD[Feature[G, Int]],
    data: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]
  ) : RDD[(Int, Iterable[Double])] = {

    groupByShapes(shapeToContextRDD(shapes, data.metadata), data)
  }

  def groupByShapes[G <: Geometry](

    // Should the data be some different format??? Where do we specify the type!?!
    shapes: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    data: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]
  ) : RDD[(Int, Iterable[Double])] = {

    shapes
      .spatialLeftOuterJoin(data)
      .flatMap{ case (k, (t1, t2)) => t1.toArray.toSeq.zip(t2.get.toArrayDouble.toSeq)}
      .groupByKey
  }


}

package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import geotrellis.vector.{Geometry, Feature, ProjectedExtent}
import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.proj4._
import geotrellis.spark._
import scala.reflect.ClassTag
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutDefinition}


object GroupByShape {

  def shapeToTile(
    vec:Geometry, w:Double, h:Double, crs: CRS, value:Int = 1
  ) : Tuple2[ProjectedExtent, Tile] = {

    val ve = vec.envelope
    val re = RasterExtent(ve, CellSize(w, h))
    val tile = Rasterizer.rasterizeWithValue(vec, re, value)
    (ProjectedExtent(ve, crs), tile) // don't project???
  }

  def shapeToContextRDD[G <: Geometry, T <: CellType](
    shapes: RDD[Feature[G, Int]],
    md: TileLayerMetadata[SpatialKey]
  ) : RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]= {

    val l = md.layout
    val crs = CRS.fromEpsgCode(4326)
    val rdd = shapes.map(f => shapeToTile(f.geom, l.cellwidth, l.cellheight, crs, f.data))
    val newMd : TileLayerMetadata[SpatialKey] = rdd.collectMetadata(crs, md.layout)
    ContextRDD(rdd.tileToLayout[SpatialKey](newMd), newMd)
  }

  def groupByVectorShapes[G <: Geometry, T <: CellGrid : ClassTag](
    shapes: RDD[Feature[G, Int]],
    data: RDD[(SpatialKey, T)] with Metadata[TileLayerMetadata[SpatialKey]]
  ) : RDD[(Int, Seq[Double])] = {

    groupByRasterShapes(shapeToContextRDD(shapes, data.metadata), data)
  }

  def groupByRasterShapes[T <: CellGrid : ClassTag](
    // Should the data be some different format??? Where do we specify the type!?!
    shapes: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
    data: RDD[(SpatialKey, T)] with Metadata[TileLayerMetadata[SpatialKey]]
  ) : RDD[(Int, Seq[Double])] = {

    shapes
      .spatialLeftOuterJoin(data)
      .flatMap{ case (k, (t1, t2)) => {

        // We want to return a Seq of doubles no matter whether it's a tile or
        // multiband tile, so we match to treat them differently. In the case
        // of Tile, we return a seq with one element.
        t2 match {
          case Some(t: Tile) =>
            t1.toArray.toSeq.zip(t.toArrayDouble.toSeq.map(Seq(_)))
          case Some(t: MultibandTile) =>
            t1.toArray.toSeq.zip(t.bands.map(_.toArrayDouble.toSeq).toSeq.transpose)
        }
      }}
  }
}

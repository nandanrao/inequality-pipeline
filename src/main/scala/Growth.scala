package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.vector._
// import Implicits._
import GroupByShape._

object Growth {

  // weighted growth is \sum \frac{y^2_{nrt}}{y_nrt-1}
  def weightedGrowth(years: RDD[(Double, Double)]) : Double = {
    years.map{ case (a,b) => b*b / a  }.sum
  }

  // create covariance???
  // convert to dataset and use .stat.cov() ??
  def cov(rdd: RDD[(Double, Long)]) : Double = ???

  // keep in raster form!!! Easier to see problems.
  // only difficulty is you need one value which is total growth...
  // nope, other difficulty is ordering...
  // Takes a simple RDD of one years nl/cap and the prev
  def growth(years: RDD[(Double, Double)]) : Double = {
    val y = years.filter{ case (a,b) => !a.isNaN && !b.isNaN } 

    val tg = y.map(_._2).sum
    val wg = weightedGrowth(years)
    val inclusiveGrowth = y.map{ case (a, b) => b/wg - a/tg }
    cov(inclusiveGrowth.zipWithIndex)
  }

  def growth[G <: Geometry](
    wealthA: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],    
    wealthB: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]],
    shapes: RDD[Feature[G, Int]]
  ) : Seq[(Int, Double)] = {

    val Seq(a,b) = Seq(wealthA, wealthB).map(groupByVectorShapes(shapes, _))

    val l = a.join(b)
      .map{ case (k, (sa, sb)) => (k, (sa(0), sb(0)))} // Get only the wealth (first band)

    val keys = a.keys.distinct.collect.toList
    val growthRates = keys.map(k => growth(l.filter(_._1 == k).values))
    keys.zip(growthRates)
  }
}

package edu.upf.inequality.pipeline

import geotrellis.raster._
import geotrellis.raster.mapalgebra.local._
import geotrellis.util.MethodExtensions

/**
 * Crushes values to 0! 
 *
 */
object Crush extends LocalTileBinaryOp {
  def combine(z1:Int,z2:Int) =
    if (isNoData(z1) || isNoData(z2)) NODATA
    else if (z1 < z2) 0
    else z1

  def combine(z1:Double,z2:Double) =
    if (z1 < z2) 0.0 
    else z1
}

trait CrushMethods extends MethodExtensions[Tile] {
  def localCrush(i: Int): Tile = Crush(self, i)
  def localCrush(d: Double): Tile = Crush(self, d)
}

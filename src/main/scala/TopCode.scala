package edu.upf.inequality.pipeline

import geotrellis.raster._
import geotrellis.raster.mapalgebra.local._
import geotrellis.util.MethodExtensions

/**
 * Censors values above a given threshold.
 *
 */
object TopCode extends LocalTileBinaryOp {
  def combine(z1:Int,z2:Int) =
    if (isNoData(z1) || isNoData(z2)) NODATA
    else if (z1 > z2) z2
    else z1

  def combine(z1:Double,z2:Double) =
    if (z1 > z2) z2 
    else z1
}

trait TopCodeMethods extends MethodExtensions[Tile] {
  def localTopCode(i: Int): Tile = TopCode(self, i)
  def localTopCode(d: Double): Tile = TopCode(self, d)
}





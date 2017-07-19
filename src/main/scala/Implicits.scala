package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
import geotrellis.raster._

import Crush._
import TopCode._

object Implicits {
  import com.redhat.et.silex.rdd.drop.implicits._
  import com.redhat.et.silex.rdd.scan.implicits._

  implicit class DoubleRDDWithCumsum(rdd: RDD[Double]) {
    def cumsum() = rdd.scanLeft(0.0)(_+_).drop(1)
  }

  implicit class withCodingMethods(val self: Tile)
      extends CrushMethods
      with TopCodeMethods
}

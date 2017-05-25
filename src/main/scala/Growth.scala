package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.util.Try

import Implicits._

object Growth {

  // weighted growth is \sum \frac{y^2_{nrt}}{y_nrt-1}
  def weightedGrowth(years: RDD[(Double, Double)]) = Double {
    years.map{ case (a,b) => b^2 / a  }.sum
  }

  // create covariance???
  // convert to dataset and use .stat.cov() ??
  def cov(rdd: RDD[Double, Int]) : Double = ???

  // keep in raster form!!! Easier to see problems.
  // only difficulty is you need one value which is total growth...
  // nope, other difficulty is ordering...
  // Takes a simple RDD of one years nl/cap and the prev
  def growth(years: RDD[(Double, Double)]) = RDD[Double]{
    val tg = years.map(_._2).sum
    val wg = weightedGrowth(years)
    val inclusiveGrowth = years.map{ case (a, b) => b/wg - a/tg }
    cov(inclusiveGrowth.zipWithIndex)
  }
}

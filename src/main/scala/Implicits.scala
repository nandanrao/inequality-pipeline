package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

object Implicits {
  import com.redhat.et.silex.rdd.drop.implicits._
  import com.redhat.et.silex.rdd.scan.implicits._

  implicit class DoubleRDDWithCumsum(rdd: RDD[Double]) {
    def cumsum() = rdd.scanLeft(0.0)(_+_).drop(1)
  }
}

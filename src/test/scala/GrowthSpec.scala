package edu.upf.inequality.pipeline

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, DataFrameSuiteBase}
import org.scalactic.TolerantNumerics
import org.scalatest.{FlatSpec, Matchers}
import Growth._
import org.apache.spark.sql.{SQLContext, SparkSession}


class GrowthTest extends FlatSpec 
    with SharedSparkContext with Matchers with RDDComparisons {


  val epsilon = .01
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  "cov" should "Should return the proper covariance" in {
    val spark = new SQLContext(sc).sparkSession
    val rdd = sc.parallelize(Seq(1.0,0.5,0.25).zip(Seq(1,2,3).map(_.toLong)))
    cov(rdd)(spark) should equal (-.375)
  }

  "Growth" should "Do something reasonable" in {
    val spark = new SQLContext(sc).sparkSession
    val rdd = sc.parallelize(Seq(1.0,1.0,0.5).zip(Seq(1.0,1.0,1.0)))
    (growth(rdd)(spark) < 1) should be (true)
  }

  "Growth" should "Handle NaNs" in {
    val spark = new SQLContext(sc).sparkSession
    val rdd = sc.parallelize(Seq(Double.NaN,1.0,1.0,0.5).zip(Seq(1.0,1.0,1.0,1.0)))
    (growth(rdd)(spark) < 1) should be (true)
  }
}

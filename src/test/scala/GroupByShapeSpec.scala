package edu.upf.inequality.pipeline

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalactic.TolerantNumerics
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.{SQLContext, SparkSession}

import Growth._
import IO._
import Gini._

class GroupByShapeSpec extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

  val epsilon = .01
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  "Grouping" should "Do reasonable things" in {
    val spark = new SQLContext(sc).sparkSession
    val wealth = readMultibandRDD(getClass.getResource("/wealth-holland.tif").toString())(sc)
    val shapes = readRDD(None, getClass.getResource("/countries-holland.tif").toString())(sc)
  }
}

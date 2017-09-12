package edu.upf.inequality.pipeline

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalactic.TolerantNumerics
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.{SQLContext, SparkSession}

import IO._
import Gini._

class GiniTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

  val epsilon = .01
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  "unweightedGini" should "Default to even weights" in {
    val l = { 1 to 20 toList } map { _.asInstanceOf[Double] }
    val rdd = sc.parallelize(l, 3)
    baseUnweightedGini(rdd)(sc) should equal (0.3166667)
  }

  "unweightedGini" should "Should give reasonable results for even population" in {
    val l = { List.fill(10)(1) } map { _.asInstanceOf[Double] }
    val rdd = sc.parallelize(l, 3)
    baseUnweightedGini(rdd)(sc) should equal (0.0)
  }

  "Gini" should " read wealth raster and give a Gini" in {
    val spark = new SQLContext(sc).sparkSession
    val wealth = readMultibandRDD(getClass.getResource("/tiny.tif").toString())(sc)
    val shapes = readRDD(None, getClass.getResource("/countries-tiny.tif").toString())(sc)
    val g = gini(wealth, shapes)(spark).take(1)(0).gini
    g should equal (0.742)
  }

  // "Gini" should "Give same results regardless of tiling strategy" in {
  //   val spark = new SQLContext(sc).sparkSession
  //   val wealth = readMultibandRDD(getClass.getResource("/wealth-holland.tif").toString(), Some(1024))(sc)
  //   val shapes = readRDD(getClass.getResource("/countries-holland.tif").toString(), Some(1024))(sc)
  //   val gA = gini(wealth, shapes)(spark).take(1)(0).gini

  //   val wealthB = readMultibandRDD(getClass.getResource("/wealth-holland.tif").toString(), Some(24))(sc)
  //   val shapesB = readRDD(getClass.getResource("/countries-holland.tif").toString(), Some(24))(sc)
  //   val gB = gini(wealthB, shapesB)(spark).take(1)(0).gini
  //   gA should equal (gB)
  // }

  "Gini" should "Takes custom weights" in {
    val l = List.fill(10)(1) map { _.asInstanceOf[Double] }
    val pop = { 1 to 10 toList } map { _.asInstanceOf[Double] }
    val rdd = sc.parallelize(l)
    baseGini(rdd, sc.parallelize(pop)) should equal (0.0)
  }

  "Gini" should "Takes custom weights v2" in {
    val l = List(1,1,5,5,10,10) map { _.asInstanceOf[Double] }
    val pop = List(10,10,1,1,10,10).map{ _.asInstanceOf[Double] }
    val g = baseGini(sc.parallelize(l), sc.parallelize(pop))
    g should equal (.410)
  }

  "Gini" should "Deal with extreme values in all ends" in {
    val l = List(0.00001,1,5,5,10,10) map { _.asInstanceOf[Double] }
    val pop = List(1000,10,1,1,10,10).map{ _.asInstanceOf[Double] }
    val g = baseGini(sc.parallelize(l), sc.parallelize(pop))
    g should equal (.978)
  }

  "Gini" should "Ignore observations with neither people nor gdp" in {
    val pure = baseGini(sc.parallelize(List(10.0,10.0)), sc.parallelize(List(5.0,10.0)))
    val l = List(0,0,0,0,0,0,0,10,10) map { _.asInstanceOf[Double] }
    val pop = List(0,0,0,0,0,0,0,5,10) map { _.asInstanceOf[Double] }
    baseGini(sc.parallelize(l), sc.parallelize(pop)) should equal (pure)
  }

  "Gini" should "Ignore observations with less than one person" in {
    val pure = baseGini(sc.parallelize(List(10.0,10.0)), sc.parallelize(List(5.0,10.0)))
    val l = List(3,2,1,1,1,1,1,10,10) map { _.asInstanceOf[Double] }
    val pop = List(0.1,0.5,0.6,0.1,0.2,0.9,0.7,5.0,10.0).map { _.asInstanceOf[Double] }
    baseGini(sc.parallelize(l), sc.parallelize(pop)) should equal (pure)
  }

  "Gini" should "Handle all 0's in NL" in {
    val l = List(0.0,0.0,0.0)
    val pop = List(1.0,1.0,1.0)
    baseGini(sc.parallelize(l), sc.parallelize(pop)) should equal (0.0)
  }

  "Gini" should "Handle all 0's in Pop" in {
    val l = List(1.0,0.0,1.0)
    val pop = List(0.0,0.0,0.0)
    baseGini(sc.parallelize(l), sc.parallelize(pop)) should equal (0.0)
  }

  "Gini" should "Handle NaNs in NL" in {
    val l = List(1.0,1.0,Double.NaN)
    val pop = List(1.0,1.0,1.0)
    baseGini(sc.parallelize(l), sc.parallelize(pop)) should equal (0.0)
  }

  "Gini" should "Handle NaNs in Pop" in {
    val l = List(1.0,1.0,1.0)
    val pop = List(1.0,1.0,Double.NaN)
    baseGini(sc.parallelize(l), sc.parallelize(pop)) should equal (0.0)
  }


  "Gini" should "Always give numbers between 0-1" in {
    val l = List(1.0,Double.PositiveInfinity,1.0, 0.5, 0.9)
    val pop = List(Double.NegativeInfinity,1.0,Double.NaN, 100, 5)
    baseGini(sc.parallelize(l), sc.parallelize(pop)) should equal (0.0)
  }

  "rotateAndApply" should "apply subtraction on rotated values" in {
    val a = 1 to 5 toSeq
    val b = 2 to 6 toSeq
    def minussum(x: Seq[Int], y: Seq[Int]) : Int = { x.zip(y).map(t => t._1 - t._2).reduce(_+_) }
    rotateAndApply(a,b, minussum) should equal (-5)
  }
}

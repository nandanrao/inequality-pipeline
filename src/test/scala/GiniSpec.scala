package edu.upf.inequality.pipeline

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalactic.TolerantNumerics
import org.scalatest.{FlatSpec, Matchers}
import Gini._

class GiniTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

  val epsilon = .01
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  "Gini" should "Default to even weights" in {
    val l = { 1 to 20 toList } map { _.asInstanceOf[Double] }
    val rdd = sc.parallelize(l, 3)
    gini(rdd)(sc) should equal (0.3166667)
  }

  "Gini" should "Should give reasonable results for even population" in {
    val l = { List.fill(10)(1) } map { _.asInstanceOf[Double] }
    val rdd = sc.parallelize(l, 3)
    gini(rdd)(sc) should equal (0.0)
  }

  "Gini" should "Takes custom weights" in {
    val l = List.fill(10)(1) map { _.asInstanceOf[Double] }
    val pop = { 1 to 10 toList } map { _.asInstanceOf[Double] }
    val rdd = sc.parallelize(l)
    gini(rdd, sc.parallelize(pop)) should equal (.3)
  }

  "Gini" should "Takes custom weights v2" in {
    val l = List(1,1,5,5,10,10) map { _.asInstanceOf[Double] }
    val pop = List(10,10,1,1,10,10).map{ _.asInstanceOf[Double] }
    val g = gini(sc.parallelize(l), sc.parallelize(pop))
    g should equal (.533)
  }

  "Gini" should "Ignore observations with neither people nor gdp" in {
    val pure = gini(sc.parallelize(List(10.0,10.0)), sc.parallelize(List(5.0,10.0)))
    val l = List(0,0,0,0,0,0,0,10,10) map { _.asInstanceOf[Double] }
    val pop = List(0,0,0,0,0,0,0,5,10) map { _.asInstanceOf[Double] }
    gini(sc.parallelize(l), sc.parallelize(pop)) should equal (pure)
  }

  "Gini" should "Ignore observations with less than one person" in {
    val pure = gini(sc.parallelize(List(10.0,10.0)), sc.parallelize(List(5.0,10.0)))
    val l = List(3,2,1,1,1,1,1,10,10) map { _.asInstanceOf[Double] }
    val pop = List(0.1,0.5,0.6,0.1,0.2,0.9,0.7,5.0,10.0)
    gini(sc.parallelize(l), sc.parallelize(pop)) should equal (pure)
  }

  "Gini" should "Handle all 0's in NL" in {
    val l = List(0.0,0.0,0.0)
    val pop = List(1.0,1.0,1.0)
    gini(sc.parallelize(l), sc.parallelize(pop)) should equal (0.0)
  }

  "Gini" should "Handle all 0's in Pop" in {
    val l = List(1.0,0.0,1.0)
    val pop = List(0.0,0.0,0.0)
    gini(sc.parallelize(l), sc.parallelize(pop)) should equal (0.0)
  }

  "Gini" should "Handle NaNs in NL" in {
    val l = List(1.0,1.0,Double.NaN)
    val pop = List(1.0,1.0,1.0)
    gini(sc.parallelize(l), sc.parallelize(pop)) should equal (0.0)
  }

  "Gini" should "Handle NaNs in Pop" in {
    val l = List(1.0,1.0,1.0)
    val pop = List(1.0,1.0,Double.NaN)
    gini(sc.parallelize(l), sc.parallelize(pop)) should equal (0.0)
  }

  "shiftzip" should "Zip with different partitions" in {
    val a = 1 to 20 toList
    val b = 21 to 40 toList
    val one = sc.parallelize(a, 3)
    val two = sc.parallelize(b, 2)
    val l = shiftzip(one,two).collect.toList.sortBy(_._1)
    l should be ((2 to 20 toList ).zip( 21 to 39 toList ))
  }
}

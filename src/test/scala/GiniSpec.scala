package edu.upf.inequality.pipeline

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}
import Gini._

class GiniTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
  // "Gini" should "Default to even weights" in {
  //   val l = { 1 to 20 toList } map { _.asInstanceOf[Double] }
  //   val rdd = sc.parallelize(l, 3)
  //   gini(rdd)(sc) should equal (1.0)
  // }
  "Shift" should "Zip with different partitions" in {
    val a = { 1 to 20 toList } map { _.asInstanceOf[Double] }
    val b = { 20 to 40 toList } map { _.asInstanceOf[Double] }
    val one = sc.parallelize(a, 3)
    val two = sc.parallelize(a, 2)
    // assertRDDEquals(shift(one, two), sc.parallelize(a).zip(sc.parallelize(b)))
    val l = shift(one,two).collect.toList
    assert(l == List(1,2,3))
  }
}

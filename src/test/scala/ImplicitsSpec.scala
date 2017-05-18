package edu.upf.inequality.pipeline

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}
import Implicits._

class ImplicitsTest extends FlatSpec with SharedSparkContext with Matchers {
  "cumsum" should "work with Doubles" in {
    val l = { 1 to 20 toList } map { _.asInstanceOf[Double] }
    val rdd = sc.parallelize(l)
    rdd.cumsum.collect.toList should be (l.scanLeft(0.0)(_+_).drop(1))
  }
}

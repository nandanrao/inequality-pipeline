package edu.upf.inequality.pipeline

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalactic.TolerantNumerics
import org.scalatest.{FlatSpec, Matchers}
import Growth._

class GiniTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

  val epsilon = .01
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  "Growth" should "Do reasonable things" in {

  }
}

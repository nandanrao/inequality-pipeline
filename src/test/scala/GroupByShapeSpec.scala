package edu.upf.inequality.pipeline

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalactic.TolerantNumerics
import org.scalatest.{FlatSpec, Matchers}
import Growth._

class GroupByShapeSpec extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

  val epsilon = .01
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  "Grouping" should "Do reasonable things" in {

  }
}

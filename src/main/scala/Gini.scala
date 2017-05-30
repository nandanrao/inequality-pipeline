package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.util.Try
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner

// import GroupByShape._
import com.redhat.et.silex.rdd.drop.implicits._
import Implicits._

object Gini {

  def gini(nl: RDD[Double])(implicit sc: SparkContext) : Double = {
    val partitions = nl.partitions.length
    val repartitioned = nl.repartition(partitions)
    gini(repartitioned, sc.parallelize(Seq.fill(nl.count.asInstanceOf[Int])(1.0), partitions))
  }

  // Our division return 0.0 if denomenator is 0 in this case
  val div = (n:Double, d:Double) => Try(n/d).getOrElse(0.0) match {
    case x if (x.isNaN) => 0.0
    case x if (x.isInfinite) => 0.0
    case x => x
  }

  val mult = (t: Tuple2[Double, Double]) => t._1*t._2

  def gini(nl: RDD[Double], pop: RDD[Double]) : Double = {
    val cleaned = nl.zip(pop)
      .filter{ case (n,p) => !n.isNaN && !p.isNaN }
      .filter{ case (n,p) => p >= 1.0 }

    val sumOfPop = cleaned.map(_._2).sum

    // (Nightlight per Population, Weights)
    val zipped = cleaned
      .map{ case (n, p) => (div(n,p), div(p,sumOfPop)) }
      .sortBy(_._1)

    val weightedNl = zipped.map(mult)
    val sumOfWeightedVals = weightedNl.sum

    val nu = weightedNl.cumsum.map(div(_, sumOfWeightedVals))
    val p = zipped.map(_._2).cumsum

    shiftzip(nu, p).map(mult).sum - shiftzip(p, nu).map(mult).sum
  }

  def shiftzip[T: ClassTag, R: ClassTag](
    one: RDD[T],
    two: RDD[R]
  ) : RDD[(T, R)] = {
    val partitioner = new HashPartitioner(one.partitions.length)

    val a = one.zipWithIndex.drop(1).map{ case (v,i) => (v, i - 1) }.map{ case (v,i) => (i, v)}
    val b = two.zipWithIndex.dropRight(1).map{ case (v,i) => (i, v)}

    a.partitionBy(partitioner).values
      .zip(b.partitionBy(partitioner).values)
  }

  // def cumsum(rdd: RDD[Double]) = rdd.scanLeft(0.0)(_+_)
}

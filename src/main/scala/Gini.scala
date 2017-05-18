package edu.upf.inequality.pipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner

// import GroupByShape._
import com.redhat.et.silex.rdd.drop.implicits._
import Implicits._

object Gini {

  def gini(nl: RDD[Double])(implicit sc: SparkContext) : Double = {
    val partitions = nl.partitions.length
    gini(nl, sc.parallelize(Seq.fill(nl.count().asInstanceOf[Int])(1), partitions))
  }

  def gini(nl: RDD[Double], pop: RDD[Double]) : Double = {
    val partitions = nl.partitions.length
    val sumOfPop = pop.reduce(_ + _)
    val weights = pop.map(_ / sumOfPop)
    val zipped = nl.zip(pop).sortBy(_._1)

    val weightedNl = zipped.map{ case (a,b) => a*b }
    val sumOfWeightedVals = weightedNl.sum

    val nu = weightedNl.cumsum.map(_ / sumOfWeightedVals)
    val p = weights.cumsum

    val a = nu.drop(1)
      .repartition(partitions)
      .zip(p.dropRight(1).repartition(partitions))
      .map{ case (a, b) => a*b}.sum

    // val b = nu.dropRight(1)
    //   .repartition(partitions)
    //   .zip(p.drop(1).repartition(partitions))
    //   .map{ case (a, b) => a*b}.sum

    a
  }

  def shift[T: ClassTag, R: ClassTag](one: RDD[T], two: RDD[R]) : RDD[(T, R)] = {
    val partitioner = new HashPartitioner(one.partitions.length)

    val a = one.zipWithIndex.drop(1).map{ case (v,i) => (v, i - 1) }.map{ case (v,i) => (i, v)}
    val b = two.zipWithIndex.dropRight(1).map{ case (v,i) => (i, v)}

    a.partitionBy(partitioner).values
      .zip(b.partitionBy(partitioner).values)
  }

  def gini(year: Int) = {
    // get rasters by year
    // run groupByShapes() 
    // run function within each shape
  }

  // def cumsum(rdd: RDD[Double]) = rdd.scanLeft(0.0)(_+_)
}


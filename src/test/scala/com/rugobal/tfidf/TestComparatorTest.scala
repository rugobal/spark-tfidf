package com.rugobal.tfidf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import com.rugobal.spark.SparkConfUtils

class TextComparatorTest extends AnyFunSuite with Matchers {

  test("test TextComparator") {

    val sparkConf = new SparkConf().setAppName("TextComparator test").setMaster("local[*]")
    SparkConfUtils.setProperties(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    {
      val docs: RDD[String] = spark.sparkContext.parallelize(Array("ruben gomez", "catherine boothman", "matt beckingham", "stephen obrien", "rubencio gonsalez"))
      val df = docs.toDF("name")

      val queryDocs = Array("rubens gomes", "catherin zothman", "raul gomsalez", "david beckham", "steven obrian", "matt beckingham")
      val queryRdd: RDD[String] = spark.sparkContext.parallelize(queryDocs)
      val queryDf = queryRdd.toDF("query")

      val start = System.currentTimeMillis

      val comparator = new TextComparator(df, "name")
      val result: SimilarityMatrix = comparator.calculateSimilarities(queryDf, "query")
      result.show

      println

      comparator.addSimilarities(queryDf, "query", threshold = 0.8).show

      val end = System.currentTimeMillis

      println(s"took ${end-start} millis to run text comparison")
    }

    {
      val docs: RDD[String] = spark.sparkContext.parallelize(Array("joan burke", "jack byrne", "jane burke", "ann sheridan", "joan byrne", "joan burk3"))
      val df = docs.toDF("name")

      val queryDocs = Array("joan burke")
      val queryRdd: RDD[String] = spark.sparkContext.parallelize(queryDocs)
      val queryDf = queryRdd.toDF("query")

      val comparator = new TextComparator(df, "name")
      val result: SimilarityMatrix = comparator.calculateSimilarities(queryDf, "query")
      result.show

      comparator.addSimilarities(queryDf, "query", threshold = 0.8).show
    }
  }

}
package com.rugobal.tfidf


import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{HashingTF, IDF, IDFModel, Normalizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class TfidfTest extends AnyFunSuite with Matchers {

  test("tf-idf model 2") {

    val sparkConf = new SparkConf().setAppName("CRDPFeatureExtractorR2").setMaster("local[*]")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    val documents: RDD[Seq[String]] = spark.sparkContext.parallelize(Array(
      str2Seq("joan burke"), str2Seq("jack byrne"), str2Seq("jane burke"), str2Seq("ann sheridan"), str2Seq("joan byrne")
    ))

    val hashingTF = new HashingTF(numFeatures = 11)
    val tf: RDD[Vector] = hashingTF.transform(documents).cache

    val idf: IDFModel = new IDF().fit(tf)
    val tfidfCorpus: RDD[Vector] = idf.transform(tf).cache

    val normalizer1 = new Normalizer()

    val tfidfCorpusNorm = tfidfCorpus.map(v => LabeledPoint(0.0, v)).map(x => (x.label, normalizer1.transform(x.features))).map(_._2)

    val corpusMatrix = toBlockMatrix(tfidfCorpusNorm)

    def query(str: String) = {
      val queryTf: RDD[Vector] = hashingTF.transform(spark.sparkContext.parallelize(Array(str2Seq(str))))
      val queryTfidf: RDD[Vector] = idf.transform(queryTf)
      val queryTfNorm = queryTfidf.map(v => LabeledPoint(0.0, v)).map(x => (x.label, normalizer1.transform(x.features))).map(_._2)

      val queryMatrix = toBlockMatrix(queryTfNorm)

      val resultQueryMatrix = queryMatrix.multiply(corpusMatrix.transpose)

      resultQueryMatrix.validate
      println
      println(s"result query matrix num rows = ${resultQueryMatrix.numRows}")
      println(s"result query matrix num cols = ${resultQueryMatrix.numCols}")
      println(s"result query matrix entries size = ${resultQueryMatrix.toCoordinateMatrix.entries.count}")

      val similarities = resultQueryMatrix.toIndexedRowMatrix().rows.filter { r => r.index == 0 }.first().vector

      println(s"similarities = ${similarities}")
    }

    //    query("rubens gomes")
    //    query("caterin botman")
    query("joan burke")
  }



  /*
   * NEW TEST WITH CUSTOM TF HASHER
   */

  test("tf-idf model with custom tf-hasher") {

    val sparkConf = new SparkConf().setAppName("Tfidf test").setMaster("local[*]")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val spark = SparkSession.builder().appName("Tfidf test").master("local[*]").config(sparkConf).getOrCreate()

    val docs: RDD[String] = spark.sparkContext.parallelize(Array("ruben gomez", "catherine boothman", "matt beckingham", "stephen obrien"))

    val hashingTf = new CustomHashingTf(docs)
    val tf = hashingTf.transform(docs)

    println("tf:")
    tf.collect.foreach(println)

    val idf: IDFModel = new IDF().fit(tf)
    val tfidfCorpus: RDD[Vector] = idf.transform(tf).cache

    //    scale(tfidfCorpus, sqlContext)
    val normalizer1 = new Normalizer()

    val tfidfCorpusNorm = tfidfCorpus.map(v => LabeledPoint(0.0, v)).map(x => (x.label, normalizer1.transform(x.features))).map(_._2)

    val corpusMatrix = toBlockMatrix(tfidfCorpusNorm)

    def query(str: String) = {
      val queryTf: RDD[Vector] = hashingTf.transform(spark.sparkContext.parallelize(Array((str))))

      println("query tf:")
      queryTf.collect.foreach(println)

      val queryTfidf: RDD[Vector] = idf.transform(queryTf)
      val queryTfNorm = queryTfidf.map(v => LabeledPoint(0.0, v)).map(x => (x.label, normalizer1.transform(x.features))).map(_._2)

      val queryMatrix = toBlockMatrix(queryTfNorm)

      val resultQueryMatrix = queryMatrix.multiply(corpusMatrix.transpose)

      resultQueryMatrix.validate
      println
      println(s"result query matrix num rows = ${resultQueryMatrix.numRows}")
      println(s"result query matrix num cols = ${resultQueryMatrix.numCols}")
      println(s"result query matrix entries size = ${resultQueryMatrix.toCoordinateMatrix.entries.count}")

      if (resultQueryMatrix.toCoordinateMatrix.entries.count > 0) {
        val similarities = resultQueryMatrix.toIndexedRowMatrix().rows.filter { r => r.index == 0 }.first().vector
        println(s"similarities = ${similarities}")
      } else {
        println("no similarities found")
      }

    }

    query("rubens gomes")
    query("caterin boothman")
    //    query("joane burk")


  }

  def toBlockMatrix(rdd: RDD[Vector]) = new IndexedRowMatrix(
    rdd.zipWithIndex.map{case (v, i) => IndexedRow(i, v)}
  ).toCoordinateMatrix.toBlockMatrix

  def str2Seq(str: String) = {
    str.toLowerCase.replaceAll("\\p{Punct}","").toSeq.map(c => c.toString())
  }

}
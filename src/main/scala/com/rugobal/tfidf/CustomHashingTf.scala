package com.rugobal.tfidf

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.broadcast.Broadcast

/**
 * Custom TF transformer. It transforms strings into Vector following the Term Frequency Algorithm.
 *
 * For more details: https://spark.apache.org/docs/latest/mllib-feature-extraction.html#tf-idf
 */
class CustomHashingTf(corpus: RDD[String]) extends Serializable {

  @transient private val sc = corpus.sparkContext
  @transient private val features: Seq[String] = corpus.flatMap(strides(_)).distinct.collect
  @transient private val featuresWithIdx: Map[String, Int] = features.zipWithIndex.toMap
  private val vectorSize = this.features.size
  private val featuresWithIdxBrd: Broadcast[Map[String, Int]] = sc.broadcast(featuresWithIdx)

  def transform(docs: RDD[String]): RDD[Vector] = docs.map(doc2Vec)

  def transform(doc: String): Vector = doc2Vec(doc)

  private def doc2Vec = (doc: String) => {
    val chunks:Seq[String] = strides(doc)
    val chunkIndices: Seq[Int] = for (chunk <- chunks if featuresWithIdxBrd.value.contains(chunk)) yield this.featuresWithIdxBrd.value(chunk)
    // some indices can be repeated if the same chunk appears more than once, so count how many instances of every chunk we have
    val vals: Seq[(Int, Double)] = chunkIndices.groupBy(identity).mapValues(_.length.toDouble).toSeq
    if (vals.isEmpty) Vectors.zeros(vectorSize) else Vectors.sparse(vectorSize, vals)
  }

  /**
   * Transforms a string into a set of strings taking n characters at a time and sliding one character.
   *
   * So calling strides("anne burke", 2) results in Seq("an","nn","ne","e ", " b", "bu", "ur", "rk", "ke")
   */
  private def strides(str: String, strideSize: Int = 2): Seq[String] = {
    val result: ArrayBuffer[String] = ArrayBuffer()
    for (idx <- (0 until str.length - strideSize + 1)) {
      result += str.substring(idx, math.min(idx+strideSize, str.length))
    }
    result.toSeq
  }
}
package com.rugobal.tfidf

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * Custom TF transformer. It transforms strings into Vector following the Term Frequency Algorithm.
 *
 * For more details: https://spark.apache.org/docs/latest/mllib-feature-extraction.html#tf-idf
 *
 * @param df: the DataFrame that contains the corpus documents.
 * @param inputCol: the name of the column to use for the corpus in the DataFrame
 * @param otpuCol: the name of the column where the TF vectors will be placed
 */
class CustomHashingTfMl(df: DataFrame, inputCol: String, outputCol: String) extends Serializable {

  @transient private val sqlContext = df.sqlContext
  @transient private val sc = sqlContext.sparkContext
  import sqlContext.implicits._
  @transient private val features: Seq[_] = df.select(inputCol).map(_.getAs[String](0)).flatMap(strides(_)).distinct.collect
  @transient private val featuresWithIdx: Map[Any, Int] = features.zipWithIndex.toMap
  private val vectorSize:Int = this.features.size
  private val featuresWithIdxBrd: Broadcast[Map[Any, Int]] = sc.broadcast(featuresWithIdx)

  /**
   * Transforms documents contained in a column of a DataFrame (which must be of type {@link StringType} to TF-Vectors.
   *
   * @param: dataset: the input DataFrame
   * @param: colName: the column where the input documents are located
   */
  def transform(dataset: DataFrame, colName: String = inputCol): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, colName)
    val t = udf { doc: String => doc2Vec(doc) }
    val metadata = outputSchema(this.outputCol).metadata
    dataset.select(col("*"), t(col(colName)).as(this.outputCol, metadata))
  }


  private def transformSchema(schema: StructType, colName: String): StructType = {
    val inputType = schema(colName).dataType
    require(inputType.isInstanceOf[StringType],  s"The input column must be StringType, but got $inputType.")
    val attrGroup = new AttributeGroup(this.outputCol, this.vectorSize)
    appendColumn(schema, attrGroup.toStructField())
    // new VectorUDT
  }

  private def doc2Vec(doc: String): Vector = {
    val chunks:Seq[String] = strides(doc)
    val chunkIndices: Seq[Int] = for (chunk <- chunks if featuresWithIdxBrd.value.contains(chunk)) yield this.featuresWithIdxBrd.value(chunk)
    // some indices can be repeated if the same chunk appears more than once, so count how many instances of every chunk we have
    val vals = chunkIndices.groupBy(identity).mapValues(_.length.toDouble).toSeq
    if (vals.isEmpty) Vectors.zeros(vectorSize) else Vectors.sparse(vectorSize, vals)
  }


  /**
   * Appends a new column to the input schema. This fails if the given output column already exists.
   * @param schema input schema
   * @param col New column schema
   * @return new schema with the input column appended
   */
  private def appendColumn(schema: StructType, col: StructField): StructType = {
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
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
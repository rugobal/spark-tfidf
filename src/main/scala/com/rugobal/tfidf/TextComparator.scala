package com.rugobal.tfidf

import org.apache.spark.ml.feature.{IDF, IDFModel, Normalizer}
import org.apache.spark.ml.linalg.{Vector}
import org.apache.spark.mllib.linalg.{Vectors => MlLibVectors}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import com.rugobal.dataframe.implicits.DataFrameExtras

/*
 * Class that provides text comparison functionality.
 *
 * It receives a column in a DataFrame, which values are the corpus documents against which later queries will be performed to calculate similarities with
 * other documents.
 *
 * Internally it uses at TF-IDF algorithm to convert documents to vectors and then it normalizes them with the L2 Norm.
 * The dot product of two L2 normalized TF-IDF vectors is the cosine similarity of the vectors. For more details see:
 *
 * https://spark.apache.org/docs/1.6.1/mllib-feature-extraction.html#tf-idf
 * https://spark.apache.org/docs/1.6.1/mllib-feature-extraction.html#normalizer
 * http://stackoverflow.com/questions/32911810/how-to-use-rowmatrix-columnsimilarities-similarity-search
 *
 * @param df: A dataframe that contains the corpus
 * @param corpusColumn: The name of the corpus column. The values MUST BE UNIQUE
 */
class TextComparator(df: DataFrame, corpusCol: String) extends Serializable {

  @transient private val _sqlContext = df.sqlContext
  @transient private val _sc = _sqlContext.sparkContext

  import df.sparkSession.implicits._

  @transient private val _dfWithIdx: DataFrame = df.select(corpusCol).distinct.zipWithIndex.cache
  @transient private val _corpusIdxMap: Map[String, Long] = _dfWithIdx.map(r => (r.getAs[String](corpusCol), r.getAs[Long]("index"))).collect.toMap

  @transient private val _hashingTF = new CustomHashingTfMl(_dfWithIdx, corpusCol, "tfVector")
  @transient val (_idfModel, _corpusMatrix) = df2CorpusMatrix(_dfWithIdx, corpusCol)


  def addSimilarities(df: DataFrame, inputCol: String, outputCol: String = "similarities", threshold: Double = 0.5) = {
    import df.sqlContext.implicits._

    val result = calculateSimilarities(df, inputCol)
    val resultDf = result.getSimilarityRows(threshold).toDF(inputCol, outputCol)

    df.join(resultDf, inputCol)
  }


  /**
   * Calculates the similarities of the give column and DataFrame with the corpus documents.
   */
  def calculateSimilarities(queryDf: DataFrame, inputCol: String): SimilarityMatrix = {

    // Check that the input column type is correct
    val inputType = queryDf.schema(inputCol).dataType
    require(inputType.isInstanceOf[StringType],  s"The input column must be StringType, but got $inputType.")

    val queryDfWithIdx: DataFrame = queryDf.select(inputCol).distinct.zipWithIndex.cache

    val queryIdxMap: Map[String, Long] = queryDfWithIdx.map(r => (r.getAs[String](inputCol), r.getAs[Long]("index"))).collect.toMap

    val queryMatrix = df2QueryMatrix(queryDfWithIdx, inputCol)

    val resultMatrix: BlockMatrix = queryMatrix.multiply(this._corpusMatrix).cache

    resultMatrix.validate

    new SimilarityMatrix(resultMatrix, queryIdxMap, this._corpusIdxMap)
  }

  private def df2CorpusMatrix(dfWithIndex: DataFrame, colName: String): (IDFModel, BlockMatrix) = {

    val tfDf = this._hashingTF.transform(dfWithIndex, colName)

    val idf = new IDF().setInputCol("tfVector").setOutputCol("tfidfVector")
    val idfModel = idf.fit(tfDf)
    val tfidfDf = idfModel.transform(tfDf)

    // Normalize the vectors with L2 Norm
    val normalizer = new Normalizer()
      .setInputCol("tfidfVector")
      .setOutputCol("normVector")
      .setP(2.0) // This is the default value anyway

    val normDf = normalizer.transform(tfidfDf).drop("tfVector").drop("tfidfVector")

    val vecIdxRdd: RDD[(Vector, Long)] = normDf.map(r => (r.getAs[Vector]("normVector"), r.getAs[Long]("index"))).rdd

    val corpusMatrix = toBlockMatrix(vecIdxRdd).transpose.cache

    (idfModel, corpusMatrix)
  }

  private def df2QueryMatrix(dfWithIndex: DataFrame, colName: String): BlockMatrix = {

    val tfDf = this._hashingTF.transform(dfWithIndex, colName)

    val tfidfDf = this._idfModel.transform(tfDf)

    // Normalize the vectors with L2 Norm
    val normalizer = new Normalizer()
      .setInputCol("tfidfVector")
      .setOutputCol("normVector")
      .setP(2.0) // This is the default value anyway

    val queryNormDf = normalizer.transform(tfidfDf).drop("tfVector").drop("tfidfVector")

    val queryVecIdxRdd: RDD[(Vector, Long)] = queryNormDf.map(r => (r.getAs[Vector]("normVector"), r.getAs[Long]("index"))).rdd

    toBlockMatrix(queryVecIdxRdd)
  }

  private def toBlockMatrix(rdd: RDD[(Vector, Long)]) = {
    new IndexedRowMatrix(rdd.map{ case (v, idx) =>
      val sv = v.toSparse
      IndexedRow(idx.toInt, MlLibVectors.sparse(sv.size, sv.indices, sv.values))
    }).toCoordinateMatrix.toBlockMatrix
  }



}





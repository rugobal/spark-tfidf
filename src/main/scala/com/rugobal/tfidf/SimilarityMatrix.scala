package com.rugobal.tfidf

import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD

/**
 * Holds the similarity matrix between 2 sets of documents and it provides useful methods to access them. It is used by {@link TextComparator}.
 */
class SimilarityMatrix(matrix: BlockMatrix, rowIndexMap: Map[String, Long], colIndexMap: Map[String, Long]) extends Serializable {

  @transient private lazy val _coordinateMatrix = matrix.toCoordinateMatrix
  @transient private val _indexedRowMatrix = matrix.toIndexedRowMatrix
  @transient private val _revColIndexMap: Map[Long, String] = colIndexMap.map { case (k,v) => (v,k) }
  @transient private val _revRowIndexMap: Map[Long, String] = rowIndexMap.map { case (k,v) => (v,k) }
  @transient private val _sc = this._indexedRowMatrix.rows.sparkContext
  private val _revColIndexMapBr = this._sc.broadcast(this._revColIndexMap)
  private val _revRowIndexMapBr = this._sc.broadcast(this._revRowIndexMap)


  private[tfidf] def getSimilarityRows(threshold: Double = 0.5): RDD[(String, Array[String])] = {
    this._indexedRowMatrix.rows.map { row => {
      val rowWord = this._revRowIndexMapBr.value(row.index)
      val sparseVec = row.vector.toSparse
      val similarities =  for (i <- (0 until sparseVec.indices.length) if sparseVec.values(i) > threshold ) yield this._revColIndexMapBr.value(sparseVec.indices(i))
      (rowWord, similarities.toArray)
    }}
  }

  /**
   * Prints the matrix in a friendly format.
   */
  def show(): Unit = {

    def colLen(col:String) = math.max(10, col.length)

    val rowDocs: Array[String] = rowIndexMap.keys.toArray.sorted
    val corpus: Array[String] = colIndexMap.keys.toArray.sorted
    val firstColLen: Int = corpus.map(_.length).max
    val header: String = s"|${pad("doc", firstColLen)}|" + corpus.map( doc => s"${pad(doc, colLen(doc))}" ).reduce(_ + "|" + _) + "|"
    val separation: String = s"+${"-" * firstColLen}+" + corpus.map(doc => s"${"-" * colLen(doc)}").reduce(_ + "+" + _) + "+"
    val entries: Array[MatrixEntry] = this._coordinateMatrix.entries.collect

    // Function to print a row
    def printRow(rowDoc: String, rowEntries: Array[MatrixEntry], corpus: Array[String]): Unit = {
      print("|") ; print(pad(rowDoc, firstColLen)) ; print("|")
      val values = corpus.map { colDoc => {
        val colIdx: Long = colIndexMap(colDoc)
        val entry = rowEntries.find(_.j == colIdx)
        val similarity = if (entry.isDefined) entry.get.value else 0.0
        pad(f"${similarity}%.5f", colLen(colDoc))
      }}
      values.foreach{v => print(v); print("|") }
      println
    }

    // Print the matrix
    println(separation)
    println(header)
    println(separation)
    rowDocs.foreach( rowDoc => {
      val rowIdx = this.rowIndexMap(rowDoc)
      val rowEntries = entries.filter(_.i == rowIdx)
      printRow(rowDoc, rowEntries, corpus)
    })
    println(separation)
  }

  private def pad(str: String, length: Int = 20): String = padL(str, length)
  private def padR(str: String, length: Int = 20): String = str.padTo(length, " ").mkString
  private def padL(str: String, length: Int = 20): String = str.reverse.padTo(length, " ").reverse.mkString

}
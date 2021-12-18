package com.rugobal.tfidf

import scala.collection.mutable.ArrayBuffer

object CustomHashingTfUtils extends Serializable {

  /**
   * Transforms a string into a set of strings taking n characters at a time and sliding one character.
   *
   * So calling strides("anne burke", 2) results in Seq("an","nn","ne","e ", " b", "bu", "ur", "rk", "ke")
   */
  private[tfidf] def strides(str: String, strideSize: Int = 2): Seq[String] = {
    val result: ArrayBuffer[String] = ArrayBuffer()
    for (idx <- (0 until str.length - strideSize + 1)) {
      result += str.substring(idx, math.min(idx+strideSize, str.length))
    }
    result.toSeq
  }

}

package com.rugobal.dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField

object implicits {

  implicit class DataFrameExtras(val df: DataFrame) {

    def zipWithIndex (implicit offset: Int = 0, colName: String = "index", inFront: Boolean = true) : DataFrame = {
      df.sqlContext.createDataFrame(
        df.rdd.zipWithIndex.map(ln =>Row.fromSeq((if (inFront) Seq(ln._2 + offset) else Seq()) ++ ln._1.toSeq ++ (if (inFront) Seq() else Seq(ln._2 + offset)))),
        StructType((if (inFront) Array(StructField(colName,LongType,false)) else Array[StructField]()) ++ df.schema.fields ++ (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,false))))
      )
    }

  }

}

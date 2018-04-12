package com.psj

import org.apache.spark.sql.SparkSession

object Concat {

  object test_Loding {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()


var middleResult = spark.sql("select " +
    "concat(A.REGIONID,'_',A.PRODUCT) as keycol, " +
    "A.REGIONID, " +
    "A.PRODUCT, " +
    "A.YEARWEEK, " +
    "cast(A.QTY as double) as QTY, " +
    "B.PRODUCTNAME " +
    "from selloutTable a " +
    "left join selloutTable1 b "+
    "on A.PRODUCT = B.PRODUCTID")







  }

}

package com.psj

import org.apache.spark.sql.SparkSession


object Concat {

  object test_Loding {
    val spark = SparkSession.builder().appName("Concat").
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

///컬럼 인덱스 생성

    var rawDataColumns = middleResult.columns

    var keycolNO = rawDataColumns.indexOf("keycol")
    var REGIONIDNO = rawDataColumns.indexOf("REGIONID")
    var PRODUCTNO = rawDataColumns.indexOf("PRODUCT")
    var YEARWEEKNO = rawDataColumns.indexOf("YEARWEEK")
    var QTYNO = rawDataColumns.indexOf("QTY")
    var PRODUCTNAMENO = rawDataColumns.indexOf("PRODUCTNAME")

    var rawRdd = middleResult.rdd





  }

}

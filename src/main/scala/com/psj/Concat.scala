package com.psj

import org.apache.spark.sql.SparkSession


object Concat {

  object test_Loding {
    val spark = SparkSession.builder().appName("Concat").
      config("spark.master", "local").
      getOrCreate()

    //Oracle XE 데이터 불러오기
    // 접속정보 설정//
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var selloutDb1 ="kopo_product_master"

    // jdbc (java database connectivity) 연결
    val selloutDataFromOracle= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load
    val selloutDataFromOracle1= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromOracle.createOrReplaceTempView("selloutTable")
    selloutDataFromOracle.show(1)

    selloutDataFromOracle1.createOrReplaceTempView("selloutTable1")
    selloutDataFromOracle1.show(1)


    var mainData = selloutDataFromOracle.rdd
    var subData = selloutDataFromOracle1.rdd

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
